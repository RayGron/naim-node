export function formatPlaneDashboardSkillsSummary(skillsSummary) {
  const enabled = skillsSummary?.enabled === true;
  const enabledCount = Number(skillsSummary?.enabled_count || 0);
  const totalCount = Number(skillsSummary?.total_count || 0);
  return {
    value: enabled ? enabledCount : 0,
    meta: enabled ? `enabled / ${totalCount} total` : "disabled",
  };
}
