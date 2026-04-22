#pragma once

#include <algorithm>
#include <string>
#include <vector>

#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

struct KnowledgeVaultCommonSkillDefinition {
  const char* id;
  const char* name;
  const char* description;
  const char* content;
  std::vector<std::string> match_terms;
};

inline const std::vector<KnowledgeVaultCommonSkillDefinition>&
KnowledgeVaultCommonSkillDefinitions() {
  static const std::vector<KnowledgeVaultCommonSkillDefinition> definitions{
      {
          "knowledge-vault-replica-search",
          "Knowledge Vault replica search",
          "Search the current plane's Knowledge Vault replica for relevant project knowledge.",
          "Use this skill when the user asks to find, recall, inspect, or search knowledge, "
          "documentation, memory, notes, or facts that should exist in the current plane's "
          "Knowledge Vault replica. Use only Knowledge Base context injected by naim-node for "
          "the current plane. Do not use global SkillsFactory data or another plane's replica. "
          "If no relevant Knowledge Vault context is available, say that the current plane's "
          "Knowledge Vault does not contain enough information.",
          {
              "knowledge vault",
              "vault",
              "kb",
              "knowledge base",
              "docs",
              "documentation",
              "memory",
              "replica",
              "search knowledge",
              "find in docs",
              "найди",
              "знания",
              "документация",
              "память",
              "реплика",
              "что известно",
          },
      },
      {
          "knowledge-vault-replica-answer-with-citations",
          "Knowledge Vault grounded answer",
          "Answer using current-plane Knowledge Vault context and preserve citations.",
          "Use this skill when the user asks a question that should be answered from the "
          "current plane's Knowledge Vault replica. Ground the answer in the injected Knowledge "
          "Base context. Cite the relevant knowledge_id and block_id when they are present. "
          "Separate facts found in Knowledge Vault from your own reasoning. Do not invent "
          "citations, source ids, or facts that are absent from the context.",
          {
              "knowledge vault",
              "grounded answer",
              "citation",
              "citations",
              "source",
              "sources",
              "provenance",
              "knowledge_id",
              "block_id",
              "ответь по базе",
              "с цитатами",
              "источник",
              "источники",
              "обоснованный ответ",
          },
      },
      {
          "knowledge-vault-replica-gap-check",
          "Knowledge Vault gap check",
          "Check whether the current plane's Knowledge Vault has enough evidence to answer.",
          "Use this skill when the user asks whether something is known, documented, confirmed, "
          "or missing in the current plane's Knowledge Vault replica. Identify what the injected "
          "Knowledge Base context supports, what remains uncertain, and what should be added to "
          "Knowledge Vault if the answer cannot be fully grounded.",
          {
              "knowledge vault",
              "gap",
              "missing",
              "unknown",
              "uncertain",
              "not documented",
              "is documented",
              "is known",
              "проверить",
              "неизвестно",
              "не хватает",
              "не задокументировано",
              "известно ли",
              "есть ли данные",
          },
      },
  };
  return definitions;
}

inline std::vector<std::string> KnowledgeVaultCommonSkillIds() {
  std::vector<std::string> ids;
  for (const auto& definition : KnowledgeVaultCommonSkillDefinitions()) {
    ids.push_back(definition.id);
  }
  return ids;
}

inline bool IsKnowledgeVaultCommonSkillEligible(const naim::DesiredState& state) {
  return state.plane_mode == naim::PlaneMode::Llm &&
         state.skills.has_value() &&
         state.skills->enabled &&
         state.knowledge.has_value() &&
         state.knowledge->enabled;
}

inline void EnsureKnowledgeVaultCommonSkillRecords(naim::ControllerStore& store) {
  for (const auto& definition : KnowledgeVaultCommonSkillDefinitions()) {
    store.UpsertSkillsFactorySkill(naim::SkillsFactorySkillRecord{
        definition.id,
        definition.name,
        "",
        definition.description,
        definition.content,
        definition.match_terms,
        false,
        "",
        "",
    });
  }
}

inline bool AttachKnowledgeVaultCommonSkills(naim::DesiredState* state) {
  if (state == nullptr || !IsKnowledgeVaultCommonSkillEligible(*state)) {
    return false;
  }

  bool changed = false;
  auto& skill_ids = state->skills->factory_skill_ids;
  for (const auto& definition : KnowledgeVaultCommonSkillDefinitions()) {
    if (std::find(skill_ids.begin(), skill_ids.end(), definition.id) !=
        skill_ids.end()) {
      continue;
    }
    skill_ids.push_back(definition.id);
    changed = true;
  }
  return changed;
}

inline bool EnsureKnowledgeVaultCommonSkills(
    naim::ControllerStore& store,
    naim::DesiredState* state) {
  EnsureKnowledgeVaultCommonSkillRecords(store);
  return AttachKnowledgeVaultCommonSkills(state);
}

}  // namespace naim::controller
