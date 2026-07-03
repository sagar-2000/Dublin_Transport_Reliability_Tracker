export function formatEta(predictedTime) {
  if (predictedTime == null) return null;
  const diffSec = predictedTime - Date.now() / 1000;
  if (diffSec <= 30) return 'Due';
  const minutes = Math.round(diffSec / 60);
  return minutes <= 1 ? '1 min' : `${minutes} min`;
}

export function formatDelay(delaySec) {
  if (delaySec == null) return null;
  if (Math.abs(delaySec) < 60) return 'On time';
  const minutes = Math.round(Math.abs(delaySec) / 60);
  return delaySec > 0 ? `${minutes} min late` : `${minutes} min early`;
}
