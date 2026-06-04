const dashboardStats = [
  {
    label: "Total des clés",
    value: "248",
    helper: "Inventaire municipal centralisé",
  },
  {
    label: "Clés disponibles",
    value: "196",
    helper: "Prêtes pour les interventions",
  },
  {
    label: "Clés sorties",
    value: "52",
    helper: "Assignées aux équipes terrain",
  },
];

const recentKeys = [
  { number: "A-104", building: "Hôtel de ville", status: "Disponible" },
  { number: "B-212", building: "Aréna municipal", status: "Sortie" },
  { number: "C-018", building: "Bibliothèque centrale", status: "Disponible" },
];

export default function HomePage() {
  return (
    <main className="home-shell">
      <section className="hero-card" aria-labelledby="page-title">
        <div className="hero-content">
          <p className="eyebrow">Tableau de bord</p>
          <h1 id="page-title">KeyGuard Municipal</h1>
          <p className="subtitle">Gestion intelligente des clés municipales</p>
        </div>

        <form className="search-panel" role="search" aria-label="Rechercher une clé municipale">
          <label htmlFor="key-search">Barre de recherche</label>
          <div className="search-row">
            <input id="key-search" name="q" type="search" placeholder="Rechercher par clé, bâtiment ou statut" />
            <button type="submit">Rechercher</button>
          </div>
        </form>
      </section>

      <section className="dashboard-section" aria-label="Indicateurs des clés">
        {dashboardStats.map((stat) => (
          <article className="stat-card" key={stat.label}>
            <span>{stat.label}</span>
            <strong>{stat.value}</strong>
            <p>{stat.helper}</p>
          </article>
        ))}
      </section>

      <section className="keys-panel" aria-labelledby="keys-title">
        <div>
          <p className="eyebrow">Suivi rapide</p>
          <h2 id="keys-title">Clés municipales récentes</h2>
        </div>
        <div className="keys-list">
          {recentKeys.map((keyItem) => (
            <div className="key-row" key={keyItem.number}>
              <div>
                <strong>{keyItem.number}</strong>
                <span>{keyItem.building}</span>
              </div>
              <p>{keyItem.status}</p>
            </div>
          ))}
        </div>
      </section>
    </main>
  );
}
