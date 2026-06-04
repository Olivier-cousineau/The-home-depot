import type { Metadata } from "next";
import KeyGuardApp from "@/components/keyguard-app";

export const metadata: Metadata = {
  title: "KeyGuard Municipal | Accueil",
  description: "Accueil de KeyGuard Municipal, l'application de gestion des clés de garde.",
};

export default function HomePage() {
  return <KeyGuardApp initialPage="dashboard" />;
}
