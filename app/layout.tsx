import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "KeyGuard Municipal",
  description: "Gestion municipale des clés de garde et recherche d'urgence.",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="fr">
      <body>{children}</body>
    </html>
  );
}
