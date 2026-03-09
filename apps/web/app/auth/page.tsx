"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { FormEvent, useEffect, useState } from "react";

import { login, signup } from "../../lib/api";
import { getSession, setSession } from "../../lib/session";

type Mode = "login" | "signup";

export default function AuthPage() {
  const router = useRouter();
  const [mode, setMode] = useState<Mode>("login");
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const existing = getSession();
    if (existing?.user.email) {
      setEmail(existing.user.email);
    }
  }, []);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const payload =
        mode === "signup"
          ? await signup({ email, name: name.trim() || email.split("@")[0] || "New User", password })
          : await login({ email, password });
      setSession(payload);
      router.push("/new-scrape");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main>
      <section className="page-header">
        <h1>{mode === "login" ? "Login" : "Sign Up"}</h1>
        <p className="page-subtitle">
          Create an account and keep projects/exports isolated per user, or login to continue.
        </p>
      </section>

      <section className="card" style={{ maxWidth: "520px" }}>
        <div className="button-row" style={{ marginBottom: "0.8rem" }}>
          <button type="button" className={mode === "login" ? "" : "button-secondary"} onClick={() => setMode("login")}>
            Login
          </button>
          <button
            type="button"
            className={mode === "signup" ? "" : "button-secondary"}
            onClick={() => setMode("signup")}
          >
            Sign Up
          </button>
        </div>

        <form className="form-grid" onSubmit={onSubmit}>
          {mode === "signup" ? (
            <label>
              Full Name
              <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Your name" />
            </label>
          ) : null}

          <label>
            Email
            <input
              type="email"
              required
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              placeholder="you@example.com"
            />
          </label>

          <label>
            Password
            <input
              type="password"
              required
              minLength={8}
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              placeholder="Minimum 8 characters"
            />
          </label>

          <div className="button-row">
            <button type="submit" disabled={loading}>
              {loading ? "Please wait..." : mode === "login" ? "Login" : "Create Account"}
            </button>
            <Link href="/" className="button-secondary" style={{ padding: "0.58rem 0.9rem" }}>
              Back
            </Link>
          </div>
        </form>

        {error ? <p className="text-danger">{error}</p> : null}
      </section>
    </main>
  );
}
