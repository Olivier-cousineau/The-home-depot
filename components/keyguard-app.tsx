"use client";

import Link from "next/link";
import { FormEvent, useMemo, useState } from "react";
import { initialKeys } from "@/lib/mock-data";
import type { BuildingRecord, KeyRecord, KeyRing, KeyStatus } from "@/lib/types";

type Page = "dashboard" | "keys" | "buildings" | "emergency";
type KeyFormState = Omit<KeyRecord, "id">;

const navigation: { id: Page; label: string; href: string }[] = [
  { id: "dashboard", label: "Tableau de bord", href: "/" },
  { id: "keys", label: "Clés", href: "/cles" },
  { id: "buildings", label: "Bâtiments", href: "/batiments" },
  { id: "emergency", label: "Mode urgence", href: "/urgence" },
];

const statuses: KeyStatus[] = ["disponible", "sortie", "perdue", "à vérifier"];
const rings: KeyRing[] = ["A", "B", "C", "D"];

const emptyForm: KeyFormState = {
  number: "",
  ring: "A",
  building: "",
  address: "",
  door: "",
  comment: "",
  status: "disponible",
};

const statusStyles: Record<KeyStatus, string> = {
  disponible: "bg-emerald-100 text-emerald-800 ring-emerald-200",
  sortie: "bg-amber-100 text-amber-800 ring-amber-200",
  perdue: "bg-rose-100 text-rose-800 ring-rose-200",
  "à vérifier": "bg-sky-100 text-sky-800 ring-sky-200",
};

function normalize(value: string) {
  return value
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function matchesSearch(key: KeyRecord, query: string) {
  const searchable = [key.number, key.building, key.address, key.door, key.ring, key.comment, key.status].join(" ");
  return normalize(searchable).includes(normalize(query));
}

function StatCard({ label, value, tone }: { label: string; value: number; tone: string }) {
  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className={`mb-4 h-2 w-16 rounded-full ${tone}`} />
      <p className="text-sm font-medium text-slate-500">{label}</p>
      <p className="mt-2 text-4xl font-bold text-municipal-900">{value}</p>
    </div>
  );
}

function StatusBadge({ status }: { status: KeyStatus }) {
  return <span className={`rounded-full px-3 py-1 text-xs font-bold ring-1 ${statusStyles[status]}`}>{status}</span>;
}

function KeyCard({ keyItem, onEdit, onDelete }: { keyItem: KeyRecord; onEdit: (keyItem: KeyRecord) => void; onDelete: (id: string) => void }) {
  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm transition hover:-translate-y-0.5 hover:shadow-municipal">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-sm font-semibold uppercase tracking-wide text-municipal-700">Anneau {keyItem.ring}</p>
          <h3 className="mt-1 text-2xl font-bold text-municipal-900">Clé #{keyItem.number}</h3>
        </div>
        <StatusBadge status={keyItem.status} />
      </div>
      <dl className="mt-5 space-y-3 text-sm text-slate-700">
        <div>
          <dt className="font-semibold text-slate-500">Bâtiment</dt>
          <dd>{keyItem.building}</dd>
        </div>
        <div>
          <dt className="font-semibold text-slate-500">Adresse</dt>
          <dd>{keyItem.address}</dd>
        </div>
        <div>
          <dt className="font-semibold text-slate-500">Porte/secteur</dt>
          <dd>{keyItem.door}</dd>
        </div>
        <div>
          <dt className="font-semibold text-slate-500">Commentaire</dt>
          <dd>{keyItem.comment || "Aucun commentaire"}</dd>
        </div>
      </dl>
      <div className="mt-5 flex flex-wrap gap-2">
        <button onClick={() => onEdit(keyItem)} className="rounded-lg bg-municipal-800 px-4 py-2 text-sm font-semibold text-white hover:bg-municipal-900">
          Modifier
        </button>
        <button onClick={() => onDelete(keyItem.id)} className="rounded-lg border border-rose-200 px-4 py-2 text-sm font-semibold text-rose-700 hover:bg-rose-50">
          Supprimer
        </button>
      </div>
    </article>
  );
}

function KeyForm({ form, setForm, editingId, onSubmit, onCancel }: { form: KeyFormState; setForm: (form: KeyFormState) => void; editingId: string | null; onSubmit: (event: FormEvent<HTMLFormElement>) => void; onCancel: () => void }) {
  const inputClass = "mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-slate-900 outline-none focus:border-municipal-500 focus:ring-4 focus:ring-municipal-100";

  return (
    <form onSubmit={onSubmit} className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="flex items-center justify-between gap-3">
        <h2 className="text-xl font-bold text-municipal-900">{editingId ? "Modifier une clé" : "Ajouter une clé"}</h2>
        {editingId && (
          <button type="button" onClick={onCancel} className="text-sm font-semibold text-slate-500 hover:text-municipal-800">
            Annuler
          </button>
        )}
      </div>
      <div className="mt-5 grid gap-4 md:grid-cols-2">
        <label className="text-sm font-semibold text-slate-700">
          Numéro de clé
          <input required value={form.number} onChange={(event) => setForm({ ...form, number: event.target.value })} className={inputClass} placeholder="42" />
        </label>
        <label className="text-sm font-semibold text-slate-700">
          Anneau
          <select value={form.ring} onChange={(event) => setForm({ ...form, ring: event.target.value as KeyRing })} className={inputClass}>
            {rings.map((ring) => (
              <option key={ring}>{ring}</option>
            ))}
          </select>
        </label>
        <label className="text-sm font-semibold text-slate-700">
          Bâtiment
          <input required value={form.building} onChange={(event) => setForm({ ...form, building: event.target.value })} className={inputClass} placeholder="Aréna municipal" />
        </label>
        <label className="text-sm font-semibold text-slate-700">
          Adresse
          <input required value={form.address} onChange={(event) => setForm({ ...form, address: event.target.value })} className={inputClass} placeholder="120, rue des Sports" />
        </label>
        <label className="text-sm font-semibold text-slate-700">
          Porte/secteur
          <input required value={form.door} onChange={(event) => setForm({ ...form, door: event.target.value })} className={inputClass} placeholder="porte mécanique arrière" />
        </label>
        <label className="text-sm font-semibold text-slate-700">
          Statut
          <select value={form.status} onChange={(event) => setForm({ ...form, status: event.target.value as KeyStatus })} className={inputClass}>
            {statuses.map((status) => (
              <option key={status}>{status}</option>
            ))}
          </select>
        </label>
        <label className="text-sm font-semibold text-slate-700 md:col-span-2">
          Commentaire
          <textarea value={form.comment} onChange={(event) => setForm({ ...form, comment: event.target.value })} className={`${inputClass} min-h-24`} placeholder="Notes pour le contremaître de garde" />
        </label>
      </div>
      <button type="submit" className="mt-5 w-full rounded-xl bg-municipal-800 px-5 py-3 font-bold text-white hover:bg-municipal-900 md:w-auto">
        {editingId ? "Enregistrer les changements" : "Ajouter la clé"}
      </button>
    </form>
  );
}

export default function KeyGuardApp({ initialPage = "dashboard" }: { initialPage?: Page }) {
  const [page, setPage] = useState<Page>(initialPage);
  const [keys, setKeys] = useState<KeyRecord[]>(initialKeys);
  const [query, setQuery] = useState("");
  const [emergencyQuery, setEmergencyQuery] = useState("");
  const [form, setForm] = useState<KeyFormState>(emptyForm);
  const [editingId, setEditingId] = useState<string | null>(null);

  const filteredKeys = useMemo(() => keys.filter((keyItem) => matchesSearch(keyItem, query)), [keys, query]);
  const emergencyResults = useMemo(() => keys.filter((keyItem) => matchesSearch(keyItem, emergencyQuery)).slice(0, 5), [keys, emergencyQuery]);
  const stats = useMemo(
    () => ({
      total: keys.length,
      available: keys.filter((keyItem) => keyItem.status === "disponible").length,
      out: keys.filter((keyItem) => keyItem.status === "sortie").length,
      review: keys.filter((keyItem) => keyItem.status === "à vérifier").length,
    }),
    [keys],
  );
  const buildings = useMemo<BuildingRecord[]>(() => {
    const grouped = new Map<string, BuildingRecord>();
    keys.forEach((keyItem) => {
      const existing = grouped.get(keyItem.building);
      if (existing) {
        existing.keyCount += 1;
        if (!existing.sectors.includes(keyItem.door)) existing.sectors.push(keyItem.door);
      } else {
        grouped.set(keyItem.building, { name: keyItem.building, address: keyItem.address, keyCount: 1, sectors: [keyItem.door] });
      }
    });
    return Array.from(grouped.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [keys]);

  function resetForm() {
    setForm(emptyForm);
    setEditingId(null);
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (editingId) {
      setKeys((current) => current.map((keyItem) => (keyItem.id === editingId ? { ...form, id: editingId } : keyItem)));
    } else {
      setKeys((current) => [{ ...form, id: `key-${Date.now()}` }, ...current]);
    }
    resetForm();
    setPage("keys");
  }

  function handleEdit(keyItem: KeyRecord) {
    setForm({
      number: keyItem.number,
      ring: keyItem.ring,
      building: keyItem.building,
      address: keyItem.address,
      door: keyItem.door,
      comment: keyItem.comment,
      status: keyItem.status,
    });
    setEditingId(keyItem.id);
    setPage("keys");
  }

  function handleDelete(id: string) {
    setKeys((current) => current.filter((keyItem) => keyItem.id !== id));
    if (editingId === id) resetForm();
  }

  function exportCsv() {
    const headers = ["Numéro", "Anneau", "Bâtiment", "Adresse", "Porte/secteur", "Commentaire", "Statut"];
    const rows = keys.map((keyItem) => [keyItem.number, keyItem.ring, keyItem.building, keyItem.address, keyItem.door, keyItem.comment, keyItem.status]);
    const csv = [headers, ...rows].map((row) => row.map((cell) => `"${cell.replaceAll('"', '""')}"`).join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "keyguard-municipal-cles.csv";
    link.click();
    URL.revokeObjectURL(url);
  }

  return (
    <main className="min-h-screen bg-slate-100">
      <header className="bg-municipal-900 text-white">
        <div className="mx-auto flex max-w-7xl flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
          <div className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.3em] text-municipal-100">Service municipal de garde</p>
              <h1 className="mt-2 text-3xl font-black sm:text-4xl">KeyGuard Municipal</h1>
              <p className="mt-2 max-w-2xl text-municipal-100">Trouvez immédiatement la bonne clé, le bon anneau et le bon accès pendant une intervention d&apos;urgence.</p>
            </div>
            <Link href="/urgence" onClick={() => setPage("emergency")} className="rounded-2xl bg-white px-5 py-3 text-center font-black text-municipal-900 shadow-lg hover:bg-municipal-50">
              Activer le mode urgence
            </Link>
          </div>
          <nav className="grid grid-cols-2 gap-2 rounded-2xl bg-white/10 p-2 md:flex">
            {navigation.map((item) => (
              <Link key={item.id} href={item.href} onClick={() => setPage(item.id)} className={`rounded-xl px-4 py-3 text-center text-sm font-bold transition ${page === item.id ? "bg-white text-municipal-900" : "text-white hover:bg-white/10"}`}>
                {item.label}
              </Link>
            ))}
          </nav>
        </div>
      </header>

      <section className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        {page === "dashboard" && (
          <div className="space-y-8">
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              <StatCard label="Total des clés" value={stats.total} tone="bg-municipal-800" />
              <StatCard label="Clés disponibles" value={stats.available} tone="bg-emerald-500" />
              <StatCard label="Clés sorties" value={stats.out} tone="bg-amber-500" />
              <StatCard label="Clés à vérifier" value={stats.review} tone="bg-sky-500" />
            </div>
            <div className="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
                <div>
                  <h2 className="text-2xl font-bold text-municipal-900">Priorités de garde</h2>
                  <p className="mt-1 text-slate-600">Surveillez les clés sorties, perdues ou nécessitant une validation.</p>
                </div>
                <button onClick={exportCsv} className="rounded-xl border border-municipal-200 px-4 py-3 font-bold text-municipal-800 hover:bg-municipal-50">
                  Export CSV
                </button>
              </div>
              <div className="mt-6 grid gap-4 lg:grid-cols-3">
                {keys.filter((keyItem) => keyItem.status !== "disponible").map((keyItem) => (
                  <KeyCard key={keyItem.id} keyItem={keyItem} onEdit={handleEdit} onDelete={handleDelete} />
                ))}
              </div>
            </div>
          </div>
        )}

        {page === "keys" && (
          <div className="grid gap-6 lg:grid-cols-[380px_1fr]">
            <KeyForm form={form} setForm={setForm} editingId={editingId} onSubmit={handleSubmit} onCancel={resetForm} />
            <div className="space-y-5">
              <div className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
                <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
                  <div>
                    <h2 className="text-2xl font-bold text-municipal-900">Répertoire des clés</h2>
                    <p className="text-slate-600">Recherche par numéro, bâtiment, adresse, porte ou anneau.</p>
                  </div>
                  <button onClick={exportCsv} className="rounded-xl bg-municipal-800 px-4 py-3 font-bold text-white hover:bg-municipal-900">
                    Export CSV
                  </button>
                </div>
                <input value={query} onChange={(event) => setQuery(event.target.value)} className="mt-5 w-full rounded-2xl border border-slate-300 px-4 py-3 outline-none focus:border-municipal-500 focus:ring-4 focus:ring-municipal-100" placeholder="Ex. 42, aréna, rue des Sports, porte mécanique, anneau B" />
              </div>
              <div className="grid gap-4 xl:grid-cols-2">
                {filteredKeys.map((keyItem) => (
                  <KeyCard key={keyItem.id} keyItem={keyItem} onEdit={handleEdit} onDelete={handleDelete} />
                ))}
              </div>
            </div>
          </div>
        )}

        {page === "buildings" && (
          <div className="space-y-5">
            <h2 className="text-3xl font-black text-municipal-900">Bâtiments</h2>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {buildings.map((building) => (
                <article key={building.name} className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
                  <p className="text-sm font-bold uppercase tracking-wide text-municipal-700">{building.keyCount} clé{building.keyCount > 1 ? "s" : ""}</p>
                  <h3 className="mt-2 text-xl font-bold text-municipal-900">{building.name}</h3>
                  <p className="mt-1 text-slate-600">{building.address}</p>
                  <div className="mt-4 flex flex-wrap gap-2">
                    {building.sectors.map((sector) => (
                      <span key={sector} className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                        {sector}
                      </span>
                    ))}
                  </div>
                </article>
              ))}
            </div>
          </div>
        )}

        {page === "emergency" && (
          <div className="rounded-3xl bg-municipal-900 p-5 text-white shadow-municipal sm:p-8">
            <p className="text-sm font-black uppercase tracking-[0.3em] text-municipal-100">Mode urgence</p>
            <h2 className="mt-3 text-3xl font-black sm:text-5xl">Quelle porte faut-il ouvrir?</h2>
            <input autoFocus value={emergencyQuery} onChange={(event) => setEmergencyQuery(event.target.value)} className="mt-8 w-full rounded-2xl border-4 border-white bg-white px-5 py-5 text-2xl font-bold text-municipal-900 outline-none focus:border-municipal-100" placeholder="Numéro, bâtiment, adresse, porte ou anneau" />
            <div className="mt-6 space-y-4">
              {emergencyQuery && emergencyResults.length === 0 && <p className="rounded-2xl bg-white/10 p-5 text-lg font-semibold">Aucun résultat. Essayez une adresse, un bâtiment ou un numéro de clé.</p>}
              {emergencyResults.map((keyItem) => (
                <div key={keyItem.id} className="rounded-2xl bg-white p-5 text-municipal-900 shadow-lg">
                  <p className="text-2xl font-black sm:text-3xl">Prendre anneau {keyItem.ring} — clé #{keyItem.number} — {keyItem.door}</p>
                  <p className="mt-3 text-lg font-semibold text-slate-700">{keyItem.building} · {keyItem.address}</p>
                  <div className="mt-4 flex flex-wrap items-center gap-3">
                    <StatusBadge status={keyItem.status} />
                    <span className="text-sm font-medium text-slate-600">{keyItem.comment}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </section>
    </main>
  );
}
