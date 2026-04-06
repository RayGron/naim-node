#include "browsing/cef_browser_backend.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "browsing/cef_support.h"

#if COMET_WITH_CEF
#include "include/cef_browser.h"
#include "include/cef_browser_process_handler.h"
#include "include/cef_devtools_message_observer.h"
#include "include/cef_frame.h"
#include "include/cef_load_handler.h"
#include "include/cef_parser.h"
#include "include/cef_render_handler.h"
#include "include/cef_request_context.h"
#include "include/cef_task.h"
#endif

namespace comet::browsing {

#if COMET_WITH_CEF
namespace {

using Clock = std::chrono::steady_clock;

constexpr int kViewWidth = 1365;
constexpr int kViewHeight = 900;
constexpr auto kLoadTimeout = std::chrono::seconds(20);
constexpr auto kPaintTimeout = std::chrono::seconds(10);
constexpr auto kFrameDataTimeout = std::chrono::seconds(10);
constexpr auto kDevToolsTimeout = std::chrono::seconds(10);
constexpr auto kRenderSettleDelay = std::chrono::milliseconds(1200);

class LambdaTask final : public CefTask {
 public:
  explicit LambdaTask(std::function<void()> fn) : fn_(std::move(fn)) {}

  void Execute() override {
    fn_();
  }

  IMPLEMENT_REFCOUNTING(LambdaTask);

 private:
  std::function<void()> fn_;
};

template <typename Callback>
void PostUiTask(Callback&& callback) {
  CefPostTask(TID_UI, new LambdaTask(std::forward<Callback>(callback)));
}

template <typename Result, typename Callback>
Result InvokeOnUiThread(Callback&& callback) {
  auto promise = std::make_shared<std::promise<Result>>();
  auto future = promise->get_future();
  PostUiTask([promise, callback = std::forward<Callback>(callback)]() mutable {
    try {
      promise->set_value(callback());
    } catch (...) {
      promise->set_exception(std::current_exception());
    }
  });
  return future.get();
}

template <typename Callback>
void InvokeOnUiThreadAndWait(Callback&& callback) {
  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future();
  PostUiTask([promise, callback = std::forward<Callback>(callback)]() mutable {
    try {
      callback();
      promise->set_value();
    } catch (...) {
      promise->set_exception(std::current_exception());
    }
  });
  future.get();
}

class PromiseStringVisitor final : public CefStringVisitor {
 public:
  explicit PromiseStringVisitor(std::shared_ptr<std::promise<std::string>> promise)
      : promise_(std::move(promise)) {}

  void Visit(const CefString& value) override {
    promise_->set_value(value.ToString());
  }

  IMPLEMENT_REFCOUNTING(PromiseStringVisitor);

 private:
  std::shared_ptr<std::promise<std::string>> promise_;
};

std::string AwaitFrameText(
    CefRefPtr<CefFrame> frame,
    bool source) {
  auto promise = std::make_shared<std::promise<std::string>>();
  auto future = promise->get_future();
  PostUiTask([frame, source, promise]() {
    if (source) {
      frame->GetSource(new PromiseStringVisitor(promise));
    } else {
      frame->GetText(new PromiseStringVisitor(promise));
    }
  });
  if (future.wait_for(kFrameDataTimeout) != std::future_status::ready) {
    return {};
  }
  return future.get();
}

std::string BasenameToken(const std::filesystem::path& path) {
  std::string value = path.filename().string();
  std::replace_if(
      value.begin(),
      value.end(),
      [](unsigned char ch) { return !std::isalnum(ch) && ch != '-' && ch != '_'; },
      '_');
  return value.empty() ? "session" : value;
}

std::filesystem::path WritePpmScreenshot(
    const std::filesystem::path& session_root,
    const std::vector<std::uint8_t>& pixels,
    int width,
    int height) {
  const auto output_path = session_root / ("snapshot-" + BasenameToken(session_root) + ".ppm");
  std::ofstream output(output_path, std::ios::binary);
  if (!output.is_open()) {
    throw std::runtime_error("failed to create rendered snapshot at " + output_path.string());
  }
  output << "P6\n" << width << " " << height << "\n255\n";
  for (int row = 0; row < height; ++row) {
    const auto* row_ptr = pixels.data() + static_cast<std::size_t>(row) * static_cast<std::size_t>(width) * 4U;
    for (int col = 0; col < width; ++col) {
      const std::uint8_t blue = row_ptr[col * 4 + 0];
      const std::uint8_t green = row_ptr[col * 4 + 1];
      const std::uint8_t red = row_ptr[col * 4 + 2];
      output.put(static_cast<char>(red));
      output.put(static_cast<char>(green));
      output.put(static_cast<char>(blue));
    }
  }
  return output_path;
}

std::filesystem::path WritePngScreenshot(
    const std::filesystem::path& session_root,
    const std::vector<std::uint8_t>& bytes) {
  const auto output_path = session_root / ("snapshot-" + BasenameToken(session_root) + ".png");
  std::ofstream output(output_path, std::ios::binary);
  if (!output.is_open()) {
    throw std::runtime_error("failed to create rendered screenshot at " + output_path.string());
  }
  output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
  return output_path;
}

class DevToolsMethodObserver final : public CefDevToolsMessageObserver {
 public:
  explicit DevToolsMethodObserver(std::shared_ptr<std::promise<std::optional<std::string>>> promise)
      : promise_(std::move(promise)) {}

  void SetMethodId(int method_id) {
    method_id_ = method_id;
  }

  void SetRegistration(CefRefPtr<CefRegistration> registration) {
    registration_ = std::move(registration);
  }

  void Fail() {
    Resolve(std::nullopt);
  }

  void OnDevToolsMethodResult(
      CefRefPtr<CefBrowser>,
      int message_id,
      bool success,
      const void* result,
      size_t result_size) override {
    if (message_id != method_id_) {
      return;
    }
    if (!success || result == nullptr || result_size == 0) {
      Resolve(std::nullopt);
      return;
    }
    Resolve(std::string(static_cast<const char*>(result), result_size));
  }

  void OnDevToolsAgentDetached(CefRefPtr<CefBrowser>) override {
    Resolve(std::nullopt);
  }

  IMPLEMENT_REFCOUNTING(DevToolsMethodObserver);

 private:
  void Resolve(std::optional<std::string> result) {
    if (resolved_.exchange(true)) {
      return;
    }
    promise_->set_value(std::move(result));
    registration_ = nullptr;
  }

  int method_id_ = 0;
  std::atomic<bool> resolved_{false};
  std::shared_ptr<std::promise<std::optional<std::string>>> promise_;
  CefRefPtr<CefRegistration> registration_;
};

std::optional<std::vector<std::uint8_t>> CaptureScreenshotWithDevTools(
    CefRefPtr<CefBrowser> browser) {
  auto promise = std::make_shared<std::promise<std::optional<std::string>>>();
  auto future = promise->get_future();

  const bool submitted = InvokeOnUiThread<bool>([browser, promise]() {
    CefRefPtr<DevToolsMethodObserver> observer = new DevToolsMethodObserver(promise);
    observer->SetRegistration(browser->GetHost()->AddDevToolsMessageObserver(observer));

    CefRefPtr<CefDictionaryValue> params = CefDictionaryValue::Create();
    params->SetString("format", "png");
    params->SetBool("fromSurface", true);
    params->SetBool("captureBeyondViewport", true);

    const int method_id = browser->GetHost()->ExecuteDevToolsMethod(0, "Page.captureScreenshot", params);
    if (method_id == 0) {
      observer->Fail();
      return false;
    }

    observer->SetMethodId(method_id);
    return true;
  });
  if (!submitted) {
    return std::nullopt;
  }
  if (future.wait_for(kDevToolsTimeout) != std::future_status::ready) {
    return std::nullopt;
  }

  const auto method_result = future.get();
  if (!method_result.has_value() || method_result->empty()) {
    return std::nullopt;
  }

  const auto parsed = nlohmann::json::parse(*method_result, nullptr, false);
  if (parsed.is_discarded()) {
    return std::nullopt;
  }
  const std::string encoded = parsed.value("data", std::string{});
  if (encoded.empty()) {
    return std::nullopt;
  }

  CefRefPtr<CefBinaryValue> decoded = CefBase64Decode(encoded);
  if (!decoded || !decoded->IsValid() || decoded->GetSize() == 0) {
    return std::nullopt;
  }

  std::vector<std::uint8_t> bytes(decoded->GetSize());
  decoded->GetData(bytes.data(), bytes.size(), 0);
  return bytes;
}

struct SessionSnapshot {
  std::string final_url;
  std::optional<std::string> title;
  std::string visible_text;
  std::string html_source;
  std::optional<std::filesystem::path> screenshot_path;
};

class SessionClient final : public CefClient,
                            public CefLifeSpanHandler,
                            public CefLoadHandler,
                            public CefRenderHandler {
 public:
  explicit SessionClient(std::filesystem::path session_root)
      : session_root_(std::move(session_root)) {}

  bool CreateBrowser(const std::string& initial_url, std::string* error_message) {
    const bool created = InvokeOnUiThread<bool>([this, initial_url]() {
      CefWindowInfo window_info;
      window_info.SetAsWindowless(0);

      CefBrowserSettings browser_settings;
      browser_settings.windowless_frame_rate = 30;

      CefRequestContextSettings context_settings;
      request_context_ = CefRequestContext::CreateContext(context_settings, nullptr);
      if (!request_context_) {
        return false;
      }

      browser_ = CefBrowserHost::CreateBrowserSync(
          window_info,
          this,
          initial_url,
          browser_settings,
          nullptr,
          request_context_);
      if (browser_) {
        browser_->GetHost()->WasResized();
      }
      return browser_ != nullptr;
    });
    if (!created) {
      if (error_message != nullptr) {
        *error_message = "failed to create CEF browser instance";
      }
      return false;
    }
    return WaitForPageReady(error_message);
  }

  bool Navigate(const std::string& url, std::string* error_message) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      load_in_progress_ = true;
      load_failed_ = false;
      last_error_message_.clear();
      last_load_change_ = Clock::now();
    }
    const bool posted = InvokeOnUiThread<bool>([this, url]() {
      if (!browser_) {
        return false;
      }
      browser_->GetMainFrame()->LoadURL(url);
      browser_->GetHost()->WasResized();
      return true;
    });
    if (!posted) {
      if (error_message != nullptr) {
        *error_message = "CEF browser session is not available";
      }
      return false;
    }
    return WaitForPageReady(error_message);
  }

  bool HasBrowser() const {
    return InvokeOnUiThread<bool>([this]() {
      return browser_ != nullptr;
    });
  }

  std::optional<SessionSnapshot> Snapshot(
      bool include_html,
      bool capture_screenshot,
      std::string* error_message) const {
    CefRefPtr<CefBrowser> browser = InvokeOnUiThread<CefRefPtr<CefBrowser>>([this]() {
      return browser_;
    });
    if (!browser) {
      if (error_message != nullptr) {
        *error_message = "CEF browser session is not available";
      }
      return std::nullopt;
    }

    if (capture_screenshot) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        has_paint_ = false;
      }
      InvokeOnUiThreadAndWait([browser]() {
        browser->GetHost()->WasHidden(false);
        browser->GetHost()->WasResized();
        browser->GetHost()->Invalidate(PET_VIEW);
      });
      std::string ignored_paint_error;
      WaitForPaint(&ignored_paint_error);
    }

    const auto page_data = InvokeOnUiThread<std::tuple<CefRefPtr<CefFrame>, std::string>>(
        [browser]() {
          CefRefPtr<CefFrame> frame = browser->GetMainFrame();
          const std::string current_url = frame ? frame->GetURL().ToString() : std::string();
          return std::make_tuple(frame, current_url);
        });
    CefRefPtr<CefFrame> frame = std::get<0>(page_data);
    const std::string visible_text = frame ? AwaitFrameText(frame, false) : std::string();
    const std::string html_source = include_html && frame ? AwaitFrameText(frame, true) : std::string();

    SessionSnapshot snapshot;
    snapshot.final_url = std::get<1>(page_data);
    snapshot.visible_text = visible_text;
    snapshot.html_source = html_source;

    if (capture_screenshot) {
      std::vector<std::uint8_t> pixels;
      int width = 0;
      int height = 0;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        pixels = pixels_;
        width = pixel_width_;
        height = pixel_height_;
      }
      if (!pixels.empty() && width > 0 && height > 0) {
        snapshot.screenshot_path = WritePpmScreenshot(session_root_, pixels, width, height);
      }
      if (!snapshot.screenshot_path.has_value()) {
        const auto png_bytes = CaptureScreenshotWithDevTools(browser);
        if (png_bytes.has_value() && !png_bytes->empty()) {
          snapshot.screenshot_path = WritePngScreenshot(session_root_, *png_bytes);
        }
      }
    }

    return snapshot;
  }

  void Close() {
    InvokeOnUiThreadAndWait([this]() {
      if (browser_) {
        browser_->GetHost()->CloseBrowser(true);
      }
    });
  }

  CefRefPtr<CefLifeSpanHandler> GetLifeSpanHandler() override {
    return this;
  }

  CefRefPtr<CefLoadHandler> GetLoadHandler() override {
    return this;
  }

  CefRefPtr<CefRenderHandler> GetRenderHandler() override {
    return this;
  }

  void OnAfterCreated(CefRefPtr<CefBrowser> browser) override {
    std::lock_guard<std::mutex> lock(mutex_);
    browser_ = browser;
  }

  void OnLoadStart(
      CefRefPtr<CefBrowser>,
      CefRefPtr<CefFrame> frame,
      TransitionType) override {
    if (!frame->IsMain()) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    load_in_progress_ = true;
    load_failed_ = false;
    last_error_message_.clear();
    last_load_change_ = Clock::now();
    cv_.notify_all();
  }

  void OnLoadEnd(
      CefRefPtr<CefBrowser>,
      CefRefPtr<CefFrame> frame,
      int) override {
    if (!frame->IsMain()) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    load_in_progress_ = false;
    last_load_change_ = Clock::now();
    cv_.notify_all();
  }

  void OnLoadError(
      CefRefPtr<CefBrowser>,
      CefRefPtr<CefFrame> frame,
      ErrorCode error_code,
      const CefString& error_text,
      const CefString&) override {
    if (!frame || !frame->IsMain()) {
      return;
    }
    if (error_code == ERR_ABORTED) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    load_in_progress_ = false;
    load_failed_ = true;
    last_error_message_ = error_text.ToString();
    last_load_change_ = Clock::now();
    cv_.notify_all();
  }

  void GetViewRect(CefRefPtr<CefBrowser>, CefRect& rect) override {
    rect = CefRect(0, 0, kViewWidth, kViewHeight);
  }

  void OnPaint(
      CefRefPtr<CefBrowser>,
      PaintElementType type,
      const RectList&,
      const void* buffer,
      int width,
      int height) override {
    if (type != PET_VIEW || buffer == nullptr || width <= 0 || height <= 0) {
      return;
    }
    const auto* bytes = static_cast<const std::uint8_t*>(buffer);
    std::vector<std::uint8_t> copy(
        bytes,
        bytes + static_cast<std::size_t>(width) * static_cast<std::size_t>(height) * 4U);
    std::lock_guard<std::mutex> lock(mutex_);
    pixels_ = std::move(copy);
    pixel_width_ = width;
    pixel_height_ = height;
    has_paint_ = true;
    cv_.notify_all();
  }

 private:
  bool WaitForPageReady(std::string* error_message) const {
    const auto deadline = Clock::now() + kLoadTimeout;
    std::unique_lock<std::mutex> lock(mutex_);
    while (true) {
      if (load_failed_) {
        if (error_message != nullptr) {
          *error_message =
              last_error_message_.empty() ? "CEF navigation failed" : last_error_message_;
        }
        return false;
      }
      if (!load_in_progress_) {
        const auto settle_deadline = last_load_change_ + kRenderSettleDelay;
        if (Clock::now() >= settle_deadline) {
          break;
        }
        if (cv_.wait_until(lock, std::min(deadline, settle_deadline)) == std::cv_status::timeout &&
            !load_in_progress_ && !load_failed_) {
          break;
        }
        continue;
      }
      if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
        if (error_message != nullptr) {
          *error_message = "timed out waiting for rendered page load";
        }
        return false;
      }
    }
    return true;
  }

  bool WaitForPaint(std::string* error_message) const {
    const auto deadline = Clock::now() + kPaintTimeout;
    std::unique_lock<std::mutex> lock(mutex_);
    while (!has_paint_) {
      if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
        if (error_message != nullptr) {
          *error_message = "timed out waiting for rendered page paint";
        }
        return false;
      }
    }
    return true;
  }

  std::filesystem::path session_root_;
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  mutable CefRefPtr<CefBrowser> browser_;
  CefRefPtr<CefRequestContext> request_context_;
  mutable bool load_in_progress_ = true;
  mutable bool load_failed_ = false;
  mutable std::string last_error_message_;
  mutable Clock::time_point last_load_change_ = Clock::now();
  mutable bool has_paint_ = false;
  mutable std::vector<std::uint8_t> pixels_;
  mutable int pixel_width_ = 0;
  mutable int pixel_height_ = 0;

  IMPLEMENT_REFCOUNTING(SessionClient);
};

}  // namespace
#endif

class CefBrowserBackend::Impl {
 public:
  explicit Impl(std::filesystem::path state_root) : state_root_(std::move(state_root)) {}

  bool IsAvailable() const {
#if COMET_WITH_CEF
    return CefRuntimeEnabled();
#else
    return false;
#endif
  }

  std::optional<CefRenderedDocument> FetchPage(
      const std::string& url,
      const std::filesystem::path& worker_root,
      std::string* error_message) const {
#if COMET_WITH_CEF
    if (!IsAvailable()) {
      if (error_message != nullptr) {
        *error_message = "CEF runtime is not initialized";
      }
      return std::nullopt;
    }
    CefRefPtr<SessionClient> client = new SessionClient(worker_root);
    if (!client->CreateBrowser(url, error_message)) {
      return std::nullopt;
    }
    auto snapshot = client->Snapshot(true, false, error_message);
    client->Close();
    if (!snapshot.has_value()) {
      return std::nullopt;
    }
    CefRenderedDocument result;
    result.final_url = snapshot->final_url;
    result.content_type = "text/html; charset=utf-8";
    result.title = snapshot->title;
    result.visible_text = snapshot->visible_text;
    result.html_source = snapshot->html_source;
    result.screenshot_path = snapshot->screenshot_path;
    return result;
#else
    (void)url;
    (void)worker_root;
    if (error_message != nullptr) {
      *error_message = "CEF runtime is not compiled into this build";
    }
    return std::nullopt;
#endif
  }

  std::optional<CefRenderedDocument> OpenSession(
      const std::string& session_id,
      const std::filesystem::path& session_root,
      const std::string& url,
      std::string* error_message) {
#if COMET_WITH_CEF
    if (!IsAvailable()) {
      if (error_message != nullptr) {
        *error_message = "CEF runtime is not initialized";
      }
      return std::nullopt;
    }

    CefRefPtr<SessionClient> client;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = sessions_.find(session_id);
      if (it == sessions_.end()) {
        client = new SessionClient(session_root);
        sessions_.emplace(session_id, client);
      } else {
        client = it->second;
      }
    }

    const bool created = client->HasBrowser();
    if (!created) {
      if (!client->CreateBrowser(url, error_message)) {
        return std::nullopt;
      }
    } else if (!client->Navigate(url, error_message)) {
      return std::nullopt;
    }

    auto snapshot = client->Snapshot(true, true, error_message);
    if (!snapshot.has_value()) {
      return std::nullopt;
    }
    CefRenderedDocument result;
    result.final_url = snapshot->final_url;
    result.content_type = "text/html; charset=utf-8";
    result.title = snapshot->title;
    result.visible_text = snapshot->visible_text;
    result.html_source = snapshot->html_source;
    if (snapshot->screenshot_path.has_value()) {
      result.screenshot_path = snapshot->screenshot_path;
    }
    return result;
#else
    (void)session_id;
    (void)session_root;
    (void)url;
    if (error_message != nullptr) {
      *error_message = "CEF runtime is not compiled into this build";
    }
    return std::nullopt;
#endif
  }

  std::optional<CefRenderedDocument> SnapshotSession(
      const std::string& session_id,
      std::string* error_message) const {
#if COMET_WITH_CEF
    CefRefPtr<SessionClient> client = FindSession(session_id);
    if (!client) {
      if (error_message != nullptr) {
        *error_message = "CEF browser session not found";
      }
      return std::nullopt;
    }
    auto snapshot = client->Snapshot(true, true, error_message);
    if (!snapshot.has_value()) {
      return std::nullopt;
    }
    CefRenderedDocument result;
    result.final_url = snapshot->final_url;
    result.title = snapshot->title;
    result.visible_text = snapshot->visible_text;
    result.html_source = snapshot->html_source;
    result.screenshot_path = snapshot->screenshot_path;
    return result;
#else
    (void)session_id;
    if (error_message != nullptr) {
      *error_message = "CEF runtime is not compiled into this build";
    }
    return std::nullopt;
#endif
  }

  std::optional<CefRenderedDocument> ExtractSession(
      const std::string& session_id,
      std::string* error_message) const {
#if COMET_WITH_CEF
    CefRefPtr<SessionClient> client = FindSession(session_id);
    if (!client) {
      if (error_message != nullptr) {
        *error_message = "CEF browser session not found";
      }
      return std::nullopt;
    }
    auto snapshot = client->Snapshot(true, false, error_message);
    if (!snapshot.has_value()) {
      return std::nullopt;
    }
    CefRenderedDocument result;
    result.final_url = snapshot->final_url;
    result.title = snapshot->title;
    result.visible_text = snapshot->visible_text;
    result.html_source = snapshot->html_source;
    return result;
#else
    (void)session_id;
    if (error_message != nullptr) {
      *error_message = "CEF runtime is not compiled into this build";
    }
    return std::nullopt;
#endif
  }

  void DeleteSession(const std::string& session_id) {
#if COMET_WITH_CEF
    CefRefPtr<SessionClient> client;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto it = sessions_.find(session_id);
      if (it == sessions_.end()) {
        return;
      }
      client = it->second;
      sessions_.erase(it);
    }
    if (client) {
      client->Close();
    }
#else
    (void)session_id;
#endif
  }

 private:
#if COMET_WITH_CEF
  CefRefPtr<SessionClient> FindSession(const std::string& session_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = sessions_.find(session_id);
    return it == sessions_.end() ? nullptr : it->second;
  }
#endif

  std::filesystem::path state_root_;
  mutable std::mutex mutex_;
#if COMET_WITH_CEF
  mutable std::unordered_map<std::string, CefRefPtr<SessionClient>> sessions_;
#endif
};

CefBrowserBackend::CefBrowserBackend(std::filesystem::path state_root)
    : state_root_(std::move(state_root)), impl_(std::make_unique<Impl>(state_root_)) {}

CefBrowserBackend::~CefBrowserBackend() = default;

bool CefBrowserBackend::IsAvailable() const {
  return impl_->IsAvailable();
}

std::optional<CefRenderedDocument> CefBrowserBackend::FetchPage(
    const std::string& url,
    const std::filesystem::path& worker_root,
    std::string* error_message) const {
  return impl_->FetchPage(url, worker_root, error_message);
}

std::optional<CefRenderedDocument> CefBrowserBackend::OpenSession(
    const std::string& session_id,
    const std::filesystem::path& session_root,
    const std::string& url,
    std::string* error_message) {
  return impl_->OpenSession(session_id, session_root, url, error_message);
}

std::optional<CefRenderedDocument> CefBrowserBackend::SnapshotSession(
    const std::string& session_id,
    std::string* error_message) const {
  return impl_->SnapshotSession(session_id, error_message);
}

std::optional<CefRenderedDocument> CefBrowserBackend::ExtractSession(
    const std::string& session_id,
    std::string* error_message) const {
  return impl_->ExtractSession(session_id, error_message);
}

void CefBrowserBackend::DeleteSession(const std::string& session_id) {
  impl_->DeleteSession(session_id);
}

}  // namespace comet::browsing
