export default function Settings() {
  return (
    <>
      <div className="page-head"><div><h2>Settings</h2><p>Demo environment — score weights match the architecture diagram</p></div></div>
      <div className="grid-2b">
        <div className="card">
          <div className="card-h"><h3>Risk aggregation weights</h3></div>
          <table>
            <tbody>
              <tr><td>AML Rules Engine</td><td>30%</td></tr>
              <tr><td>NVIDIA Transaction Foundation Model (512-d)</td><td>35%</td></tr>
              <tr><td>XGBoost (CPU histogram)</td><td>20%</td></tr>
              <tr><td>KYC &amp; Risk Enrichment</td><td>15%</td></tr>
            </tbody>
          </table>
        </div>
        <div className="card">
          <div className="card-h"><h3>Score bands</h3></div>
          <table>
            <tbody>
              <tr><td>Low</td><td>0–39</td></tr>
              <tr><td>Medium</td><td>40–69</td></tr>
              <tr><td>High</td><td>70–84</td></tr>
              <tr><td>Critical</td><td>85–100</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
