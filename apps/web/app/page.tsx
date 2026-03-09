import Link from "next/link";

export default function HomePage() {
  return (
    <main>
      <section className="page-header">
        <h1>Scraping Control Center</h1>
        <p className="page-subtitle">
          Launch preview runs, track full extraction jobs, review templates, and download signed export artifacts.
        </p>
      </section>

      <section className="grid-cards">
        <article className="card">
          <h2>New Scrape</h2>
          <p>Run preview extraction, edit field labels with confidence scores, and start full jobs.</p>
          <Link href="/new-scrape">Open New Scrape</Link>
        </article>

        <article className="card">
          <h2>Job History</h2>
          <p>Monitor queue status, retries, and current progress for all jobs in one table.</p>
          <Link href="/jobs">Open Jobs</Link>
        </article>

        <article className="card">
          <h2>Template Library</h2>
          <p>Inspect versions, success rates, and invalidation health for reusable templates.</p>
          <Link href="/templates">Open Templates</Link>
        </article>

        <article className="card">
          <h2>File Manager</h2>
          <p>Access generated artifacts and signed download links for CSV and JSON exports.</p>
          <Link href="/file-manager">Open File Manager</Link>
        </article>
      </section>

      <section className="card card-muted" style={{ marginTop: "1rem" }}>
        <h2>Quick Path</h2>
        <p>
          Fastest loop: run preview, adjust field names, start full run, then watch status updates and export from the
          job detail page.
        </p>
      </section>
    </main>
  );
}
