"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { clearSession, getSession, sessionEventName, type SessionState } from "../../lib/session";

const NAV_ITEMS = [
  { href: "/", label: "Dashboard" },
  { href: "/new-scrape", label: "New Scrape" },
  { href: "/jobs", label: "Jobs" },
  { href: "/file-manager", label: "File Manager" },
  { href: "/templates", label: "Templates" },
];

export function TopNav() {
  const pathname = usePathname();
  const router = useRouter();
  const [session, setSession] = useState<SessionState | null>(null);

  useEffect(() => {
    const sync = () => setSession(getSession());
    sync();
    window.addEventListener(sessionEventName(), sync);
    return () => window.removeEventListener(sessionEventName(), sync);
  }, []);

  return (
    <header className="top-nav-wrap">
      <div className="top-nav-inner">
        <Link href="/" className="brand-link">
          WebScrapper
        </Link>
        <nav className="top-nav-links" aria-label="Main navigation">
          {NAV_ITEMS.map((item) => {
            const isActive = pathname === item.href || (item.href !== "/" && pathname.startsWith(item.href));
            return (
              <Link key={item.href} href={item.href} className={isActive ? "nav-link nav-link-active" : "nav-link"}>
                {item.label}
              </Link>
            );
          })}
        </nav>
        <div className="top-nav-user">
          {session ? (
            <>
              <span className="meta-chip">{session.user.email || session.user.user_id}</span>
              <button
                type="button"
                className="button-secondary"
                onClick={() => {
                  clearSession();
                  router.push("/auth");
                }}
              >
                Logout
              </button>
            </>
          ) : (
            <Link href="/auth" className="nav-link nav-link-active">
              Login / Sign Up
            </Link>
          )}
        </div>
      </div>
    </header>
  );
}
