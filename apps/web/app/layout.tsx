import type { Metadata } from "next";
import "./globals.css";
import { TopNav } from "./components/top-nav";

export const metadata: Metadata = {
  title: "WebScrapper",
  description: "AI-assisted web scraper MVP",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>
        <TopNav />
        {children}
      </body>
    </html>
  );
}
