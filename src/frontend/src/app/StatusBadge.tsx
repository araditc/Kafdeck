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
  >
    <bdi dir="auto">{label ?? presentation.label}</bdi>
    <span className="visually-hidden">: {presentation.description}</span>
  </span>;
}
