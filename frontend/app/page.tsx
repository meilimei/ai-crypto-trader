const metrics = [
  { label: "Backend", status: "Healthy", detail: "Responding to health checks" },
  { label: "PostgreSQL", status: "Configured", detail: "Running via docker-compose" },
  { label: "LLM Agent", status: "Pending", detail: "Connect model provider to activate" }
];

export default function Home(): JSX.Element {
  return (
    <main className="page">
      <header className="hero">
        <div>
          <p className="eyebrow">AI Crypto Trader</p>
          <h1>Operational dashboard</h1>
          <p className="lede">
            Track service health and iterate on trading logic, data collection, and LLM decision
            support from a single monorepo.
          </p>
        </div>
        <div className="pill">Monorepo: frontend · backend · infra</div>
      </header>

      <section className="panel">
        <div className="panel__header">
          <h2>System status</h2>
          <p className="muted">High-level view of core services.</p>
        </div>
        <div className="grid">
          {metrics.map(({ label, status, detail }) => (
            <article key={label} className="card">
              <div className="card__row">
                <h3>{label}</h3>
                <span className="badge">{status}</span>
              </div>
              <p className="muted">{detail}</p>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
