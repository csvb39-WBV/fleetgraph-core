import React from "react";


type Props = {
  predictedDemandScore: number;
  confidenceLevel: string;
  forecastWindow: string;
};


export function PredictiveInsightsSummary({
  predictedDemandScore,
  confidenceLevel,
  forecastWindow,
}: Props): JSX.Element {
  return (
    <section aria-label="Predictive Insights Summary">
      <h3>Summary</h3>
      <ul>
        <li>Predicted Demand Score: {predictedDemandScore}</li>
        <li>Confidence Level: {confidenceLevel}</li>
        <li>Forecast Window: {forecastWindow}</li>
      </ul>
    </section>
  );
}


export default PredictiveInsightsSummary;