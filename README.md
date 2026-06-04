# KeyGuard Municipal

Application web Next.js pour aider un contremaître municipal de garde à trouver rapidement la bonne clé en cas d'urgence.

## Démarrage

```bash
npm install
npm run dev
```

Ouvrez ensuite http://localhost:3000.

## Fonctionnalités

- Interface en français, responsive mobile.
- Tableau de bord avec total des clés, clés disponibles, sorties et à vérifier.
- Répertoire des clés avec recherche par numéro, bâtiment, adresse, porte/secteur ou anneau.
- Ajout, modification et suppression de clés avec données locales mockées.
- Statuts pris en charge : disponible, sortie, perdue, à vérifier.
- Anneaux pris en charge : A, B, C, D.
- Vue bâtiments regroupée par adresse et secteurs.
- Mode urgence avec résultat clair, par exemple : « Prendre anneau B — clé #42 — porte mécanique arrière ».
- Export CSV du registre des clés.

## Stack

- Next.js 15
- React
- TypeScript
- Tailwind CSS
