import { uiStatusPresentation, type UiStatusKind } from './statusPresentation.js';

export function StatusBadge({
  kind,
  label,
}: {
  kind: UiStatusKind;
  label?: string;
}) {
  const presentation = uiStatusPresentation(kind);
  return <span
    className={`badge ${presentation.badgeClass}`}
    title={presentation.description}
    aria-label={`${label ?? presentation.label}: ${presentation.description}`}
  >
    {label ?? presentation.label}
  </span>;
}
