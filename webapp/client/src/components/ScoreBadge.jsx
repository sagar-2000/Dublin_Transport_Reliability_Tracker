import { scoreColor, scoreLabel } from '../lib/scoreLookup.js';

export function ScoreBadge({ score }) {
  return (
    <span className="score-badge" style={{ backgroundColor: scoreColor(score) }}>
      {scoreLabel(score)}
    </span>
  );
}
