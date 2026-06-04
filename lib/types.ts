export type KeyStatus = "disponible" | "sortie" | "perdue" | "à vérifier";
export type KeyRing = "A" | "B" | "C" | "D";

export type KeyRecord = {
  id: string;
  number: string;
  ring: KeyRing;
  building: string;
  address: string;
  door: string;
  comment: string;
  status: KeyStatus;
};

export type BuildingRecord = {
  name: string;
  address: string;
  keyCount: number;
  sectors: string[];
};
