import { describe, expect, it } from "vitest";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

import {
  buildDesiredStateV2FromForm,
  buildNewPlaneFormState,
  buildPlaneFormStateFromDesiredStateV2,
  validatePlaneV2Form,
} from "./planeV2Form.jsx";
import { PlaneEditorDialog } from "./App.jsx";
import {
  buildSkillsFactoryGroupTree,
  collectGroupSkillIds,
  filterPlaneSelectableSkills,
  filterSkillsFactoryItems,
  formatSkillGroupPath,
  NO_GROUP_TREE_PATH,
  sortSkillsFactoryItems,
} from "./skillsFactory.js";
import { formatPlaneDashboardSkillsSummary } from "./planeSkills.js";

describe("skillsFactory utils", () => {
  const items = [
    {
      id: "skill-zeta",
      name: "Zeta",
      group_path: "code-agent/debugging",
      description: "Blue answer",
      content: "Always answer BLUE",
      plane_count: 1,
      plane_names: ["maglev"],
    },
    {
      id: "skill-alpha",
      name: "Alpha",
      group_path: "code-agent",
      description: "Shared answer",
      content: "Always answer ALPHA",
      plane_count: 3,
      plane_names: ["maglev", "demo", "prod"],
    },
  ];

  it("sorts by plane count before name", () => {
    expect(sortSkillsFactoryItems(items, "plane_count").map((item) => item.id)).toEqual([
      "skill-alpha",
      "skill-zeta",
    ]);
  });

  it("filters across id, content, and plane usage", () => {
    expect(filterSkillsFactoryItems(items, "blue").map((item) => item.id)).toEqual([
      "skill-zeta",
    ]);
    expect(filterPlaneSelectableSkills(items, "prod").map((item) => item.id)).toEqual([
      "skill-alpha",
    ]);
  });

  it("builds a group tree and filters by subtree", () => {
    const tree = buildSkillsFactoryGroupTree(items, [{ path: "lt-jex/localtrade/account" }]);
    expect(tree.total_skill_count).toBe(2);
    expect(tree.children.map((item) => item.path)).toEqual(["code-agent", "lt-jex"]);
    expect(tree.children[0].children.map((item) => item.path)).toEqual(["code-agent/debugging"]);
    expect(filterSkillsFactoryItems(items, "", "code-agent").map((item) => item.id)).toEqual([
      "skill-zeta",
      "skill-alpha",
    ]);
    expect(
      filterSkillsFactoryItems(items, "", "code-agent/debugging").map((item) => item.id),
    ).toEqual(["skill-zeta"]);
    expect(collectGroupSkillIds(items, "code-agent")).toEqual(["skill-zeta", "skill-alpha"]);
  });

  it("formats empty group paths as no group", () => {
    expect(formatSkillGroupPath("")).toBe("No group");
    expect(formatSkillGroupPath(" localtrade / streams ")).toBe("localtrade/streams");
  });

  it("adds No group node and filters ungrouped items", () => {
    const tree = buildSkillsFactoryGroupTree([
      ...items,
      {
        id: "skill-ungrouped",
        name: "Ungrouped",
        group_path: "",
        description: "No group skill",
        content: "No group content",
        plane_count: 0,
        plane_names: [],
      },
    ]);
    expect(tree.children.map((item) => item.path)).toEqual(["code-agent", NO_GROUP_TREE_PATH]);
    expect(filterSkillsFactoryItems(items, "", NO_GROUP_TREE_PATH)).toEqual([]);
    expect(
      filterSkillsFactoryItems(
        [
          ...items,
          {
            id: "skill-ungrouped",
            name: "Ungrouped",
            group_path: "",
            description: "No group skill",
            content: "No group content",
            plane_count: 0,
            plane_names: [],
          },
        ],
        "",
        NO_GROUP_TREE_PATH,
      ).map((item) => item.id),
    ).toEqual(["skill-ungrouped"]);
  });
});

describe("planeV2Form SkillsFactory mapping", () => {
  it("derives served model and server names from plane name by default", () => {
    const form = buildNewPlaneFormState();
    form.planeName = "Maglev Web";

    const desiredState = buildDesiredStateV2FromForm(form);
    expect(desiredState.model.served_model_name).toBe("maglev-web");
    expect(desiredState.network.server_name).toBe("maglev-web.local");
  });

  it("round-trips factory skill ids through desired state v2", () => {
    const form = buildNewPlaneFormState();
    form.planeName = "skills-plane";
    form.modelPath = "/models/qwen";
    form.skillsEnabled = true;
    form.factorySkillIds = ["skill-alpha", "skill-beta"];

    const desiredState = buildDesiredStateV2FromForm(form);
    expect(desiredState.skills).toEqual({
      enabled: true,
      factory_skill_ids: ["skill-alpha", "skill-beta"],
    });

    const reparsed = buildPlaneFormStateFromDesiredStateV2(desiredState);
    expect(reparsed.factorySkillIds).toEqual(["skill-alpha", "skill-beta"]);
  });

  it("warns when factory selections exist while Skills is disabled", () => {
    const form = buildNewPlaneFormState();
    form.modelPath = "/models/qwen";
    form.factorySkillIds = ["skill-alpha"];
    const validation = validatePlaneV2Form(form);
    expect(validation.warnings).toContain(
      "Selected Skills Factory records are ignored until Skills is enabled.",
    );
  });

  it("round-trips browsing capability through desired state v2", () => {
    const form = buildNewPlaneFormState();
    form.planeName = "browsing-plane";
    form.modelPath = "/models/qwen";
    form.browsingEnabled = true;
    form.browserSessionEnabled = true;

    const desiredState = buildDesiredStateV2FromForm(form);
    expect(desiredState.browsing).toEqual({
      enabled: true,
      policy: {
        browser_session_enabled: true,
      },
    });

    const reparsed = buildPlaneFormStateFromDesiredStateV2(desiredState);
    expect(reparsed.browsingEnabled).toBe(true);
    expect(reparsed.browserSessionEnabled).toBe(true);
  });

  it("round-trips placement execution node and external app host through desired state v2", () => {
    const form = buildNewPlaneFormState();
    form.planeName = "placement-ui-plane";
    form.modelPath = "/models/qwen";
    form.executionNode = "worker-node-a";
    form.appEnabled = true;
    form.appImage = "example/app:dev";
    form.appHostEnabled = true;
    form.appHostAddress = "10.0.0.15";
    form.appHostAuthMode = "ssh-key";
    form.appHostSshKeyPath = "/home/test/.ssh/id_ed25519";

    const desiredState = buildDesiredStateV2FromForm(form);
    expect(desiredState.placement).toEqual({
      execution_node: "worker-node-a",
      app_host: {
        address: "10.0.0.15",
        ssh_key_path: "/home/test/.ssh/id_ed25519",
      },
    });

    const reparsed = buildPlaneFormStateFromDesiredStateV2(desiredState);
    expect(reparsed.executionNode).toBe("worker-node-a");
    expect(reparsed.appHostEnabled).toBe(true);
    expect(reparsed.appHostAddress).toBe("10.0.0.15");
    expect(reparsed.appHostAuthMode).toBe("ssh-key");
    expect(reparsed.appHostSshKeyPath).toBe("/home/test/.ssh/id_ed25519");
  });

  it("loads legacy placement primary_node as execution node", () => {
    const form = buildPlaneFormStateFromDesiredStateV2({
      version: 2,
      plane_name: "legacy-placement-plane",
      plane_mode: "compute",
      placement: {
        primary_node: "worker-node-a",
      },
      runtime: {
        engine: "custom",
        workers: 1,
      },
    });

    expect(form.executionNode).toBe("worker-node-a");
  });

  it("does not emit legacy node-placement fields when topology is disabled", () => {
    const form = buildNewPlaneFormState();
    form.planeName = "placement-clean-plane";
    form.modelPath = "/models/qwen";
    form.executionNode = "worker-node-a";
    form.inferOverridesEnabled = true;
    form.inferNode = "legacy-infer-node";
    form.workerNode = "legacy-worker-node";
    form.workerAssignmentsEnabled = true;
    form.workerAssignments = [{ node: "legacy-worker-node", gpuDevice: "0" }];
    form.appEnabled = true;
    form.appImage = "example/app:dev";
    form.appNode = "legacy-app-node";
    form.topologyEnabled = false;

    const desiredState = buildDesiredStateV2FromForm(form);
    expect(desiredState.placement).toEqual({
      execution_node: "worker-node-a",
    });
    expect(desiredState.infer.node).toBeUndefined();
    expect(desiredState.worker.node).toBeUndefined();
    expect(desiredState.worker.assignments).toBeUndefined();
    expect(desiredState.app.node).toBeUndefined();
  });

  it("serializes and reparses worker-prepared library model selection", () => {
    const form = buildNewPlaneFormState();
    form.planeName = "ux-plane-smoke";
    form.modelRef = "Qwen/Qwen3.5-9B-Q8_0";
    form.materializationMode = "prepare_on_worker";
    form.materializationLocalPath =
      "/mnt/array/naim/storage/gguf/Qwen/Qwen3.5-9B-Q8_0/Qwen3.5-9B-Q8_0.gguf";
    form.materializationSourceNodeName = "storage1";
    form.materializationSourcePaths = [
      "/mnt/array/naim/storage/gguf/Qwen/Qwen3.5-9B-Q8_0/Qwen3.5-9B-Q8_0.gguf",
    ];
    form.materializationSourceFormat = "gguf";
    form.materializationSourceQuantization = "Q8_0";
    form.modelQuantization = "Q8_0";
    form.modelTargetFilename = "Qwen3.5-9B-Q8_0.gguf";
    form.servedModelName = "ux-plane-smoke-qwen";
    form.servedModelNameManual = true;
    form.executionNode = "hpc1";
    form.workerGpuDevice = "1";
    form.gatewayPort = 18284;
    form.inferencePort = 18294;

    const validation = validatePlaneV2Form(form);
    expect(validation.errors).toEqual([]);

    const desiredState = buildDesiredStateV2FromForm(form);
    expect(desiredState.model).toMatchObject({
      source: {
        type: "library",
        ref: "Qwen/Qwen3.5-9B-Q8_0",
      },
      served_model_name: "ux-plane-smoke-qwen",
      materialization: {
        mode: "prepare_on_worker",
        source_node_name: "storage1",
        source_format: "gguf",
        source_quantization: "Q8_0",
        quantization: "Q8_0",
      },
      target_filename: "Qwen3.5-9B-Q8_0.gguf",
    });
    expect(desiredState.network.gateway_port).toBe(18284);
    expect(desiredState.network.inference_port).toBe(18294);

    const reparsed = buildPlaneFormStateFromDesiredStateV2(desiredState);
    expect(reparsed.modelRef).toBe("Qwen/Qwen3.5-9B-Q8_0");
    expect(reparsed.materializationSourceNodeName).toBe("storage1");
    expect(reparsed.materializationSourcePaths).toEqual(form.materializationSourcePaths);
    expect(reparsed.modelQuantization).toBe("Q8_0");
    expect(reparsed.workerGpuDevice).toBe("1");
  });

  it("validates required execution-node fields for external app host", () => {
    const form = buildNewPlaneFormState();
    form.modelPath = "/models/qwen";
    form.executionNode = "";
    form.appHostEnabled = true;

    const validation = validatePlaneV2Form(form);
    expect(validation.errors).toContain("Execution node is required.");
    expect(validation.errors).toContain("External app host requires the app container to be enabled.");
    expect(validation.errors).toContain("External app host address is required.");
    expect(validation.errors).toContain("External app host SSH key path is required.");
  });

  it("warns when browser sessions are enabled without browsing", () => {
    const form = buildNewPlaneFormState();
    form.modelPath = "/models/qwen";
    form.browserSessionEnabled = true;
    const validation = validatePlaneV2Form(form);
    expect(validation.warnings).toContain(
      "Browser sessions are ignored until Isolated Browsing is enabled.",
    );
  });

  it("requires worker image and start for compute planes", () => {
    const form = buildNewPlaneFormState();
    form.planeMode = "compute";
    const validation = validatePlaneV2Form(form);
    expect(validation.errors).toContain("Worker image is required for compute planes.");
    expect(validation.errors).toContain("Worker start is required for compute planes.");
  });
});

describe("PlaneEditorDialog", () => {
  it("renders with explicit skillsFactoryGroups prop", () => {
    const html = renderToStaticMarkup(
      React.createElement(PlaneEditorDialog, {
        dialog: {
          open: true,
          mode: "new",
          planeName: "",
          planeState: "",
          text: "{}",
          form: buildNewPlaneFormState(),
          originalSkillsEnabled: false,
          busy: false,
          error: "",
        },
        setDialog: () => {},
        onClose: () => {},
        onSave: () => {},
        modelLibraryItems: [],
        skillsFactoryItems: [],
        skillsFactoryGroups: [],
      }),
    );

    expect(html).toContain("New plane");
    expect(html).toContain("Isolated Browsing");
    expect(html).toContain("Generated JSON");
    expect(html).not.toContain("Runtime engine");
  });
});

describe("plane dashboard skills summary", () => {
  it("formats disabled skills summary", () => {
    expect(
      formatPlaneDashboardSkillsSummary({
        enabled: false,
        enabled_count: 3,
        total_count: 5,
      }),
    ).toEqual({
      value: 0,
      meta: "disabled",
    });
  });

  it("formats enabled skills summary with counts", () => {
    expect(
      formatPlaneDashboardSkillsSummary({
        enabled: true,
        enabled_count: 2,
        total_count: 5,
      }),
    ).toEqual({
      value: 2,
      meta: "enabled / 5 total",
    });
  });
});
