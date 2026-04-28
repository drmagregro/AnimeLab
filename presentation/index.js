const pptxgen = require("pptxgenjs");
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";
pres.title = "Anime Data Pipeline — Du CSV à Elasticsearch";
pres.author = "Data Team";

// ── Palette ───────────────────────────────────────────────────────────────────
const C = {
  dark: "0D1117", // presque noir
  dark2: "161B22", // fond card
  dark3: "21262D", // fond section
  border: "30363D", // bordures subtiles
  teal: "00B4D8", // accent principal
  teal2: "0077B6", // accent secondaire
  teal3: "ADE8F4", // texte accentué clair
  amber: "F4A261", // warning / highlight
  green: "2EA043", // succès
  purple: "8B5CF6", // ELK
  coral: "F87171", // erreur / attention
  white: "F0F6FC", // texte principal
  muted: "8B949E", // texte secondaire
  muted2: "6E7681", // texte tertiaire
  tag_bg: "1C2D3F", // fond des badges
};

// Phase colors
const PHASES = {
  audit: { color: C.amber, label: "Data Refinement", bg: "1C1A0D" },
  clean: { color: C.green, label: "Data Refinement", bg: "0D1C0F" },
  elk: { color: C.purple, label: "Big Data — ELK", bg: "160D2A" },
  airflow: { color: C.teal, label: "ETL & Pipelines", bg: "0D1C22" },
  extract: { color: C.teal, label: "ETL & Pipelines", bg: "0D1C22" },
  transform: { color: C.teal2, label: "ETL & Pipelines", bg: "0D1822" },
  load: { color: C.coral, label: "ETL & Pipelines", bg: "1C0D0D" },
};

// ─── Helper functions ──────────────────────────────────────────────────────────

function addDarkBg(slide, color) {
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0,
    y: 0,
    w: 10,
    h: 5.625,
    fill: { color: color || C.dark },
    line: { color: color || C.dark },
  });
}

function addTopBar(slide, phaseColor, phaseLabel, sessionInfo) {
  // Bar de phase coloré (fin)
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0,
    y: 0,
    w: 10,
    h: 0.055,
    fill: { color: phaseColor },
    line: { color: phaseColor },
  });
  // Phase label top right
  slide.addText(phaseLabel.toUpperCase(), {
    x: 6.5,
    y: 0.08,
    w: 3.2,
    h: 0.25,
    fontSize: 7,
    bold: true,
    color: phaseColor,
    align: "right",
    margin: 0,
    charSpacing: 2,
  });
  if (sessionInfo) {
    slide.addText(sessionInfo, {
      x: 0.4,
      y: 0.08,
      w: 4,
      h: 0.25,
      fontSize: 7,
      color: C.muted2,
      margin: 0,
      charSpacing: 1,
    });
  }
}

function addSlideTitle(slide, title, subtitle) {
  slide.addText(title, {
    x: 0.45,
    y: 0.35,
    w: 9.1,
    h: 0.75,
    fontSize: 26,
    bold: true,
    color: C.white,
    margin: 0,
  });
  if (subtitle) {
    slide.addText(subtitle, {
      x: 0.45,
      y: 1.05,
      w: 8,
      h: 0.35,
      fontSize: 13,
      color: C.muted,
      margin: 0,
      italic: true,
    });
  }
}

function addCard(slide, x, y, w, h, accentColor) {
  slide.addShape(pres.shapes.RECTANGLE, {
    x,
    y,
    w,
    h,
    fill: { color: C.dark2 },
    line: { color: accentColor || C.border, width: 0.75 },
  });
  if (accentColor) {
    slide.addShape(pres.shapes.RECTANGLE, {
      x,
      y,
      w: 0.06,
      h,
      fill: { color: accentColor },
      line: { color: accentColor },
    });
  }
}

function addTag(slide, x, y, text, color) {
  const w = text.length * 0.085 + 0.25;
  slide.addShape(pres.shapes.RECTANGLE, {
    x,
    y,
    w,
    h: 0.22,
    fill: { color: C.tag_bg },
    line: { color: color || C.teal, width: 0.5 },
  });
  slide.addText(text, {
    x,
    y,
    w,
    h: 0.22,
    fontSize: 7.5,
    color: color || C.teal,
    align: "center",
    margin: 0,
    bold: true,
  });
  return w + 0.1;
}

function addBullets(slide, items, x, y, w, h, color, iconColor) {
  const parts = [];
  items.forEach((item, i) => {
    parts.push({
      text: "▸  ",
      options: { color: iconColor || C.teal, bold: true },
    });
    parts.push({
      text: item,
      options: { color: color || C.white, breakLine: i < items.length - 1 },
    });
  });
  slide.addText(parts, {
    x,
    y,
    w,
    h,
    fontSize: 12,
    lineSpacingMultiple: 1.5,
    margin: 0,
  });
}

function addStat(slide, x, y, w, value, label, color) {
  addCard(slide, x, y, w, 0.95, color);
  slide.addText(value, {
    x: x + 0.12,
    y: y + 0.1,
    w: w - 0.15,
    h: 0.5,
    fontSize: 28,
    bold: true,
    color: color || C.teal,
    margin: 0,
    align: "center",
  });
  slide.addText(label, {
    x: x + 0.12,
    y: y + 0.6,
    w: w - 0.15,
    h: 0.25,
    fontSize: 9,
    color: C.muted,
    margin: 0,
    align: "center",
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLIDE 1 — COVER
// ═══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);

  // Glow décoratif
  s.addShape(pres.shapes.OVAL, {
    x: 6.5,
    y: -0.5,
    w: 5,
    h: 5,
    fill: { color: "00B4D8", transparency: 88 },
    line: { color: "00B4D8", transparency: 88 },
  });

  s.addText("ANIME DATA PIPELINE", {
    x: 0.6,
    y: 1.1,
    w: 8.8,
    h: 0.6,
    fontSize: 9,
    bold: true,
    color: C.teal,
    charSpacing: 6,
    margin: 0,
  });

  s.addText("Du CSV brut à\nElasticsearch", {
    x: 0.6,
    y: 1.7,
    w: 8.5,
    h: 1.6,
    fontSize: 44,
    bold: true,
    color: C.white,
    margin: 0,
    lineSpacingMultiple: 1.15,
  });

  s.addText(
    "Audit · Nettoyage · Feature Engineering · ELK Stack · Apache Airflow",
    {
      x: 0.6,
      y: 3.35,
      w: 8.5,
      h: 0.35,
      fontSize: 12,
      color: C.muted,
      margin: 0,
    },
  );

  // Timeline chips
  const sessions = [
    { label: "Lun. 23/03 AM", color: C.amber },
    { label: "Mar. 24/03 AM", color: C.green },
    { label: "Mar. 24/03 PM", color: C.purple },
    { label: "Mer. 25/03", color: C.teal },
    { label: "Jeu. 26/03", color: C.teal2 },
  ];
  let cx = 0.6;
  sessions.forEach(({ label, color }) => {
    const w = label.length * 0.087 + 0.3;
    s.addShape(pres.shapes.RECTANGLE, {
      x: cx,
      y: 3.9,
      w,
      h: 0.26,
      fill: { color: C.dark2 },
      line: { color: color, width: 0.75 },
    });
    s.addText(label, {
      x: cx,
      y: 3.9,
      w,
      h: 0.26,
      fontSize: 8,
      color: color,
      align: "center",
      margin: 0,
    });
    cx += w + 0.1;
  });

  s.addText("7 sessions · 3h30 chacune · 24h30 de formation", {
    x: 0.6,
    y: 5.2,
    w: 8.8,
    h: 0.25,
    fontSize: 8.5,
    color: C.muted2,
    margin: 0,
  });

  // Barre de couleur en bas
  const barColors = [
    C.amber,
    C.green,
    C.purple,
    C.teal,
    C.teal,
    C.teal2,
    C.coral,
  ];
  barColors.forEach((c, i) => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: i * (10 / 7),
      y: 5.555,
      w: 10 / 7,
      h: 0.07,
      fill: { color: c },
      line: { color: c },
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLIDE 2 — AGENDA GÉNÉRAL
// ═══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(s, C.teal, "Vue d'ensemble", "Agenda complet");
  addSlideTitle(
    s,
    "Programme de la semaine",
    "7 sessions — 3 thèmes — 1 pipeline complet",
  );

  const sessions = [
    {
      day: "Lun. 23/03 — PM",
      title: "Audit & Découverte du dataset brut",
      dur: "3h30",
      color: C.amber,
      phase: "Data Refinement",
    },
    {
      day: "Mar. 24/03 — AM",
      title: "Nettoyage, enrichissement & Feature Engineering",
      dur: "3h30",
      color: C.green,
      phase: "Data Refinement",
    },
    {
      day: "Mar. 24/03 — PM",
      title: "Découverte de la stack ELK & Indexation",
      dur: "3h30",
      color: C.purple,
      phase: "Big Data — ELK",
    },
    {
      day: "Mer. 25/03 — AM",
      title: "Introduction à Apache Airflow & Premier DAG",
      dur: "3h30",
      color: C.teal,
      phase: "ETL & Pipelines",
    },
    {
      day: "Mer. 25/03 — PM",
      title: "Pipeline Extract : lecture multi-fichiers & validation",
      dur: "3h30",
      color: C.teal,
      phase: "ETL & Pipelines",
    },
    {
      day: "Jeu. 26/03 — AM",
      title: "Pipeline Transform : nettoyage & feature engineering auto",
      dur: "3h30",
      color: C.teal2,
      phase: "ETL & Pipelines",
    },
    {
      day: "Jeu. 26/03 — PM",
      title: "Pipeline Load : indexation Elasticsearch & monitoring",
      dur: "3h30",
      color: C.coral,
      phase: "ETL & Pipelines",
    },
  ];

  sessions.forEach((sess, i) => {
    const y = 1.5 + i * 0.565;
    // Fond row
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.35,
      y,
      w: 9.3,
      h: 0.5,
      fill: { color: i % 2 === 0 ? C.dark2 : C.dark3 },
      line: { color: C.border, width: 0.5 },
    });
    // Couleur gauche
    s.addShape(pres.shapes.RECTANGLE, {
      x: 0.35,
      y,
      w: 0.055,
      h: 0.5,
      fill: { color: sess.color },
      line: { color: sess.color },
    });
    // Numéro
    s.addText(`${i + 1}`, {
      x: 0.45,
      y: y + 0.05,
      w: 0.3,
      h: 0.4,
      fontSize: 11,
      bold: true,
      color: sess.color,
      margin: 0,
      align: "center",
    });
    // Jour
    s.addText(sess.day, {
      x: 0.8,
      y: y + 0.05,
      w: 1.7,
      h: 0.4,
      fontSize: 9,
      color: C.muted,
      margin: 0,
      valign: "middle",
    });
    // Titre
    s.addText(sess.title, {
      x: 2.55,
      y: y + 0.05,
      w: 4.8,
      h: 0.4,
      fontSize: 10.5,
      bold: true,
      color: C.white,
      margin: 0,
      valign: "middle",
    });
    // Phase
    s.addText(sess.phase, {
      x: 7.4,
      y: y + 0.12,
      w: 1.6,
      h: 0.26,
      fontSize: 7.5,
      color: sess.color,
      align: "center",
      margin: 0,
    });
    // Durée
    s.addText(sess.dur, {
      x: 9.05,
      y: y + 0.12,
      w: 0.55,
      h: 0.26,
      fontSize: 9,
      bold: true,
      color: C.muted2,
      margin: 0,
      align: "center",
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLIDES SECTIONS — helper pour header de section
// ═══════════════════════════════════════════════════════════════════════════════
function makeSectionSlide(
  num,
  title,
  subtitle,
  date,
  phase,
  phaseColor,
  bgColor,
) {
  const s = pres.addSlide();
  addDarkBg(s, bgColor);
  // Bande verticale gauche
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0,
    y: 0,
    w: 0.35,
    h: 5.625,
    fill: { color: phaseColor },
    line: { color: phaseColor },
  });
  // Numéro session
  s.addText(`SESSION ${num}`, {
    x: 0.6,
    y: 1.0,
    w: 4,
    h: 0.4,
    fontSize: 8,
    bold: true,
    color: phaseColor,
    charSpacing: 4,
    margin: 0,
  });
  s.addText(title, {
    x: 0.6,
    y: 1.45,
    w: 8.8,
    h: 1.1,
    fontSize: 30,
    bold: true,
    color: C.white,
    margin: 0,
    lineSpacingMultiple: 1.15,
  });
  s.addText(subtitle, {
    x: 0.6,
    y: 2.65,
    w: 7.5,
    h: 0.45,
    fontSize: 14,
    color: C.muted,
    italic: true,
    margin: 0,
  });
  // Date + phase
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0.6,
    y: 3.3,
    w: 2.8,
    h: 0.35,
    fill: { color: C.dark2 },
    line: { color: phaseColor, width: 0.75 },
  });
  s.addText(`${date}  ·  3h30`, {
    x: 0.6,
    y: 3.3,
    w: 2.8,
    h: 0.35,
    fontSize: 9.5,
    color: phaseColor,
    align: "center",
    margin: 0,
  });
  s.addText(phase.toUpperCase(), {
    x: 3.6,
    y: 3.35,
    w: 3,
    h: 0.25,
    fontSize: 8,
    color: C.muted2,
    charSpacing: 2,
    margin: 0,
  });
  // Glow
  s.addShape(pres.shapes.OVAL, {
    x: 7,
    y: 2,
    w: 4,
    h: 4,
    fill: { color: phaseColor, transparency: 90 },
    line: { color: phaseColor, transparency: 90 },
  });
  // Barre bas
  s.addShape(pres.shapes.RECTANGLE, {
    x: 0,
    y: 5.555,
    w: 10,
    h: 0.07,
    fill: { color: phaseColor },
    line: { color: phaseColor },
  });
  return s;
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 1 — AUDIT & DÉCOUVERTE
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  1,
  "Audit & Découverte\ndu Dataset Brut",
  "Comprendre les données avant de les transformer",
  "Lundi 23/03 — PM",
  "Data Refinement",
  C.amber,
  "0F0D05",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.amber,
    "Data Refinement",
    "Session 1 · Lundi 23/03 PM · 13h30–17h00",
  );
  addSlideTitle(
    s,
    "Les 3 sources de données",
    "Dataset MyAnimeList — ~12 000 anime, 73 000 utilisateurs",
  );

  const sources = [
    {
      file: "anime.csv",
      cols: "anime_id, name, genre, type, episodes, rating, members",
      role: "Catalogue principal",
      color: C.amber,
    },
    {
      file: "anime_with_synopsis.csv",
      cols: "MAL_ID, Name, Synopsis, Genres, Premiered",
      role: "Descriptions enrichies",
      color: C.green,
    },
    {
      file: "rating.csv",
      cols: "user_id, anime_id, rating",
      role: "Notations communauté",
      color: C.teal,
    },
  ];

  sources.forEach((src, i) => {
    const y = 1.5 + i * 1.25;
    addCard(s, 0.35, y, 9.3, 1.1, src.color);
    s.addText(src.file, {
      x: 0.55,
      y: y + 0.12,
      w: 3,
      h: 0.35,
      fontSize: 14,
      bold: true,
      color: src.color,
      fontFace: "Consolas",
      margin: 0,
    });
    s.addText(src.role, {
      x: 0.55,
      y: y + 0.55,
      w: 3,
      h: 0.35,
      fontSize: 10,
      color: C.muted,
      margin: 0,
      italic: true,
    });
    s.addText(src.cols, {
      x: 3.9,
      y: y + 0.22,
      w: 5.5,
      h: 0.6,
      fontSize: 9.5,
      color: C.muted,
      fontFace: "Consolas",
      margin: 0,
      lineSpacingMultiple: 1.5,
    });
  });
}

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(s, C.amber, "Data Refinement", "Session 1 · Audit de qualité");
  addSlideTitle(
    s,
    "Métriques d'audit collectées",
    "Chaque CSV est analysé selon 5 dimensions de qualité",
  );

  const metrics = [
    {
      icon: "📊",
      title: "Valeurs manquantes",
      desc: "% par colonne\n→ Seuil d'alerte > 50%",
      color: C.amber,
    },
    {
      icon: "🔁",
      title: "Doublons",
      desc: "Lignes identiques\n→ Dédup sur anime_id",
      color: C.coral,
    },
    {
      icon: "🔢",
      title: "Types de données",
      desc: "Détection automatique\nPandas dtypes",
      color: C.teal,
    },
    {
      icon: "⚖️",
      title: "Distribution",
      desc: "Statistiques descriptives\nmin / max / moyenne",
      color: C.green,
    },
    {
      icon: "🔗",
      title: "Cohérence inter-fichiers",
      desc: "IDs communs\nAnime sans synopsis",
      color: C.purple,
    },
    {
      icon: "💾",
      title: "Volumétrie",
      desc: "Taille mémoire\nNb lignes × colonnes",
      color: C.teal2,
    },
  ];

  metrics.forEach((m, i) => {
    const col = i % 3;
    const row = Math.floor(i / 3);
    const x = 0.35 + col * 3.15;
    const y = 1.55 + row * 1.55;
    addCard(s, x, y, 2.95, 1.35, m.color);
    s.addText(m.title, {
      x: x + 0.15,
      y: y + 0.15,
      w: 2.7,
      h: 0.35,
      fontSize: 11.5,
      bold: true,
      color: C.white,
      margin: 0,
    });
    s.addText(m.desc, {
      x: x + 0.15,
      y: y + 0.55,
      w: 2.7,
      h: 0.65,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
      lineSpacingMultiple: 1.4,
    });
  });
}

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(s, C.amber, "Data Refinement", "Session 1 · Rapport d'audit");
  addSlideTitle(
    s,
    "Rapport d'audit — audit_report.json",
    "Sortie structurée : métriques + alertes + contrôles croisés",
  );

  // Code block
  addCard(s, 0.35, 1.45, 5.1, 3.85, C.amber);
  s.addText(
    [
      { text: "{\n", options: { color: C.white } },
      { text: '  "anime": {\n', options: { color: C.teal3 } },
      { text: '    "rows": ', options: { color: C.muted } },
      { text: "12294", options: { color: C.amber } },
      { text: ',\n    "duplicates": ', options: { color: C.muted } },
      { text: "0", options: { color: C.green } },
      {
        text: ',\n    "null_pct": {\n      "rating": ',
        options: { color: C.muted },
      },
      { text: "28.3", options: { color: C.coral } },
      { text: ',\n      "episodes": ', options: { color: C.muted } },
      { text: "2.1", options: { color: C.green } },
      { text: "\n    }\n  },\n", options: { color: C.muted } },
      { text: '  "cross_checks": {\n', options: { color: C.teal3 } },
      { text: '    "anime_with_synopsis": ', options: { color: C.muted } },
      { text: "11680", options: { color: C.teal } },
      { text: ',\n    "rated_not_in_catalog": ', options: { color: C.muted } },
      { text: "47\n  }\n}", options: { color: C.amber } },
    ],
    {
      x: 0.52,
      y: 1.55,
      w: 4.78,
      h: 3.6,
      fontFace: "Consolas",
      fontSize: 10,
      margin: 0,
      lineSpacingMultiple: 1.55,
    },
  );

  // Key findings
  const findings = [
    {
      label: "28.3%",
      desc: "de ratings manquants dans anime.csv",
      color: C.coral,
    },
    {
      label: "73 515",
      desc: "utilisateurs uniques dans rating.csv",
      color: C.teal,
    },
    {
      label: "47",
      desc: "anime notés hors catalogue principal",
      color: C.amber,
    },
    {
      label: "-1",
      desc: 'valeur spéciale = "vu mais non noté"',
      color: C.purple,
    },
  ];
  findings.forEach((f, i) => {
    const y = 1.5 + i * 0.92;
    addCard(s, 5.65, y, 4.0, 0.78, f.color);
    s.addText(f.label, {
      x: 5.85,
      y: y + 0.1,
      w: 1.2,
      h: 0.55,
      fontSize: 20,
      bold: true,
      color: f.color,
      margin: 0,
      align: "center",
    });
    s.addText(f.desc, {
      x: 7.0,
      y: y + 0.18,
      w: 2.5,
      h: 0.42,
      fontSize: 10,
      color: C.muted,
      margin: 0,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 2 — NETTOYAGE & FEATURE ENGINEERING
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  2,
  "Nettoyage, Enrichissement\n& Feature Engineering",
  "Transformer les données brutes en données prêtes à l'emploi",
  "Mardi 24/03 — AM",
  "Data Refinement",
  C.green,
  "050F07",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.green,
    "Data Refinement",
    "Session 2 · Mardi 24/03 AM · 09h00–12h30",
  );
  addSlideTitle(
    s,
    "Pipeline de nettoyage par dataset",
    "3 fichiers, 3 stratégies de nettoyage ciblées",
  );

  const cleanSteps = [
    {
      file: "anime.csv",
      color: C.amber,
      steps: [
        "Déduplication sur anime_id (keep='first')",
        'Genre : string → liste Python ["Action", "Drama"]',
        "episodes / rating → pd.to_numeric(errors='coerce')",
        "type → fillna('Unknown').str.upper()",
      ],
    },
    {
      file: "anime_with_synopsis.csv",
      color: C.green,
      steps: [
        "Rename MAL_ID → anime_id",
        'Suppression "No synopsis information..."',
        "Strip whitespace + normalisation unicode",
        "Jointure préservée via left merge",
      ],
    },
    {
      file: "rating.csv",
      color: C.teal,
      steps: [
        "Exclusion des -1 (vu non noté)",
        "Clip rating entre 1 et 10",
        "Déduplication user_id × anime_id (keep='last')",
        "Typage forcé int pour user_id, anime_id, rating",
      ],
    },
  ];

  cleanSteps.forEach((ds, i) => {
    const y = 1.5 + i * 1.3;
    addCard(s, 0.35, y, 9.3, 1.15, ds.color);
    s.addText(ds.file, {
      x: 0.55,
      y: y + 0.1,
      w: 2.5,
      h: 0.35,
      fontSize: 11.5,
      bold: true,
      color: ds.color,
      fontFace: "Consolas",
      margin: 0,
    });
    const parts = [];
    ds.steps.forEach((step, j) => {
      parts.push({ text: "→ ", options: { color: ds.color, bold: true } });
      parts.push({
        text: step,
        options: { color: C.muted, breakLine: j < ds.steps.length - 1 },
      });
    });
    s.addText(parts, {
      x: 3.2,
      y: y + 0.1,
      w: 6.2,
      h: 0.95,
      fontSize: 9.5,
      margin: 0,
      lineSpacingMultiple: 1.45,
    });
  });
}

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(s, C.green, "Data Refinement", "Session 2 · Feature Engineering");
  addSlideTitle(
    s,
    "Features créées — Enrichissement",
    "4 nouvelles colonnes à valeur analytique élevée",
  );

  const features = [
    {
      name: "quality_score",
      type: "float [0–10]",
      color: C.green,
      formula:
        "rating × 0.5 + user_mean × 0.3 + log(members)/log(max) × 10 × 0.2",
      desc: "Score composite pondéré combinant note officielle MAL, note communauté et popularité (log-normalisée)",
    },
    {
      name: "popularity_tier",
      type: "catégorielle",
      color: C.amber,
      formula:
        "blockbuster ≥500K | popular ≥100K | known ≥10K | niche ≥1K | obscure",
      desc: "Segmentation des anime en 5 niveaux de popularité selon le nombre de membres",
    },
    {
      name: "user_rating_stats",
      type: "mean / count / std",
      color: C.teal,
      formula:
        "ratings.groupby('anime_id')['rating'].agg(['mean','count','std'])",
      desc: "Agrégation des notations utilisateurs : permet de détecter les anime sur- ou sous-notés",
    },
    {
      name: "tags",
      type: "list[str]",
      color: C.purple,
      formula: "Mapping genre → tag normalisé (ex: 'Sci-Fi' → 'sci-fi')",
      desc: "Tags normalisés prêts pour l'indexation Elasticsearch et la recherche full-text",
    },
  ];

  features.forEach((f, i) => {
    const y = 1.5 + i * 0.98;
    addCard(s, 0.35, y, 9.3, 0.88, f.color);
    s.addText(f.name, {
      x: 0.55,
      y: y + 0.1,
      w: 2.2,
      h: 0.3,
      fontSize: 11,
      bold: true,
      color: f.color,
      fontFace: "Consolas",
      margin: 0,
    });
    s.addText(f.type, {
      x: 0.55,
      y: y + 0.5,
      w: 2.2,
      h: 0.28,
      fontSize: 8.5,
      color: C.muted2,
      margin: 0,
      italic: true,
    });
    s.addText(f.formula, {
      x: 2.95,
      y: y + 0.1,
      w: 6.5,
      h: 0.28,
      fontSize: 9,
      color: f.color,
      fontFace: "Consolas",
      margin: 0,
    });
    s.addText(f.desc, {
      x: 2.95,
      y: y + 0.44,
      w: 6.5,
      h: 0.32,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 3 — ELK
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  3,
  "Découverte de la Stack ELK\n& Indexation",
  "Elasticsearch, Logstash, Kibana — du JSON à la recherche",
  "Mardi 24/03 — PM",
  "Big Data — ELK",
  C.purple,
  "0A0614",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.purple,
    "Big Data — ELK",
    "Session 3 · Mardi 24/03 PM · 13h30–17h00",
  );
  addSlideTitle(
    s,
    "Architecture ELK Stack",
    "Elasticsearch · Logstash · Kibana",
  );

  const components = [
    {
      name: "Elasticsearch",
      role: "Moteur de recherche & stockage",
      detail: "Index JSON · Shards · Replicas\nQueriesHTTP REST API",
      color: C.teal,
      x: 0.4,
    },
    {
      name: "Logstash",
      role: "Ingestion & transformation",
      detail: "Input → Filter → Output\nPlugins CSV / JSON / Grok",
      color: C.purple,
      x: 3.55,
    },
    {
      name: "Kibana",
      role: "Visualisation & exploration",
      detail: "Dashboards · Dev Tools\nDiscover · Lens · Maps",
      color: C.amber,
      x: 6.7,
    },
  ];

  components.forEach((c) => {
    addCard(s, c.x, 1.55, 2.95, 2.4, c.color);
    s.addText(c.name, {
      x: c.x + 0.15,
      y: 1.75,
      w: 2.65,
      h: 0.42,
      fontSize: 16,
      bold: true,
      color: c.color,
      margin: 0,
      align: "center",
    });
    s.addText(c.role, {
      x: c.x + 0.15,
      y: 2.2,
      w: 2.65,
      h: 0.3,
      fontSize: 10,
      color: C.muted,
      margin: 0,
      align: "center",
      italic: true,
    });
    s.addShape(pres.shapes.RECTANGLE, {
      x: c.x + 0.2,
      y: 2.6,
      w: 2.55,
      h: 0.02,
      fill: { color: c.color, transparency: 60 },
      line: { color: c.color, transparency: 60 },
    });
    s.addText(c.detail, {
      x: c.x + 0.2,
      y: 2.72,
      w: 2.55,
      h: 0.9,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
      lineSpacingMultiple: 1.5,
      align: "center",
    });
  });

  // Flèches entre
  [3.35, 6.5].forEach((x) => {
    s.addText("→", {
      x: x - 0.05,
      y: 2.5,
      w: 0.25,
      h: 0.4,
      fontSize: 18,
      color: C.muted2,
      margin: 0,
      align: "center",
    });
  });

  // Note bas
  s.addText(
    "Dans ce projet : anime_full.json → Logstash → Index Elasticsearch → Dashboards Kibana",
    {
      x: 0.4,
      y: 4.3,
      w: 9.2,
      h: 0.3,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
      align: "center",
      italic: true,
    },
  );
}

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(s, C.purple, "Big Data — ELK", "Session 3 · Mapping & indexation");
  addSlideTitle(
    s,
    "Mapping Elasticsearch — Index Anime",
    "Schéma d'index optimisé pour la recherche full-text",
  );

  addCard(s, 0.35, 1.45, 5.3, 3.85, C.purple);
  s.addText(
    [
      { text: "PUT /anime\n{\n  ", options: { color: C.white } },
      {
        text: '"mappings": {\n    "properties": {\n',
        options: { color: C.teal3 },
      },
      { text: '      "anime_id": ', options: { color: C.muted } },
      { text: '{ "type": "integer" },\n', options: { color: C.green } },
      { text: '      "name": ', options: { color: C.muted } },
      {
        text: '{ "type": "text",\n        "fields": { "keyword": {...} } },\n',
        options: { color: C.amber },
      },
      { text: '      "genre": ', options: { color: C.muted } },
      { text: '{ "type": "keyword" },\n', options: { color: C.green } },
      { text: '      "synopsis": ', options: { color: C.muted } },
      {
        text: '{ "type": "text",\n        "analyzer": "french" },\n',
        options: { color: C.teal },
      },
      { text: '      "quality_score":', options: { color: C.muted } },
      { text: '{ "type": "float" }\n', options: { color: C.purple } },
      { text: "    }\n  }\n}", options: { color: C.white } },
    ],
    {
      x: 0.55,
      y: 1.6,
      w: 4.9,
      h: 3.55,
      fontFace: "Consolas",
      fontSize: 9.5,
      margin: 0,
      lineSpacingMultiple: 1.5,
    },
  );

  const kibana_points = [
    {
      title: "Dev Tools",
      desc: "Requêtes DSL directes sur l'index",
      color: C.purple,
    },
    {
      title: "Discover",
      desc: "Exploration des documents indexés",
      color: C.teal,
    },
    { title: "Lens", desc: "Visualisation drag-and-drop", color: C.amber },
    {
      title: "Dashboards",
      desc: "Top anime, répartition par genre",
      color: C.green,
    },
  ];

  kibana_points.forEach((p, i) => {
    const y = 1.5 + i * 0.92;
    addCard(s, 5.85, y, 3.8, 0.78, p.color);
    s.addText(p.title, {
      x: 6.05,
      y: y + 0.1,
      w: 3.4,
      h: 0.3,
      fontSize: 12,
      bold: true,
      color: p.color,
      margin: 0,
    });
    s.addText(p.desc, {
      x: 6.05,
      y: y + 0.44,
      w: 3.4,
      h: 0.25,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 4 — INTRODUCTION AIRFLOW
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  4,
  "Introduction à Apache Airflow\n& Premier DAG",
  "Orchestration de pipelines — concepts fondamentaux",
  "Mercredi 25/03 — AM",
  "ETL & Pipelines",
  C.teal,
  "050F10",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.teal,
    "ETL & Pipelines",
    "Session 4 · Mercredi 25/03 AM · 09h00–12h30",
  );
  addSlideTitle(
    s,
    "Concepts Airflow fondamentaux",
    "DAG · Task · Operator · XCom · Scheduler",
  );

  const concepts = [
    {
      term: "DAG",
      full: "Directed Acyclic Graph",
      desc: "Graphe orienté sans cycle définissant l'ordre d'exécution des tâches",
      color: C.teal,
    },
    {
      term: "Task",
      full: "Unité de travail",
      desc: "Instance d'un Operator. Chaque tâche = une étape du pipeline",
      color: C.green,
    },
    {
      term: "Operator",
      full: "Template de tâche",
      desc: "PythonOperator · BashOperator · EmailOperator · S3Operator...",
      color: C.amber,
    },
    {
      term: "XCom",
      full: "Cross-Communication",
      desc: "Mécanisme de passage de données entre tâches via ti.xcom_push/pull",
      color: C.purple,
    },
    {
      term: "Scheduler",
      full: "Ordonnanceur",
      desc: "Déclenche les DAGs selon le schedule_interval (@daily, @weekly, cron)",
      color: C.coral,
    },
    {
      term: "Executor",
      full: "Moteur d'exécution",
      desc: "LocalExecutor · CeleryExecutor · KubernetesExecutor",
      color: C.teal2,
    },
  ];

  concepts.forEach((c, i) => {
    const col = i % 2;
    const row = Math.floor(i / 2);
    const x = 0.35 + col * 4.75;
    const y = 1.5 + row * 1.32;
    addCard(s, x, y, 4.5, 1.18, c.color);
    s.addText(c.term, {
      x: x + 0.15,
      y: y + 0.1,
      w: 1.3,
      h: 0.4,
      fontSize: 16,
      bold: true,
      color: c.color,
      fontFace: "Consolas",
      margin: 0,
    });
    s.addText(c.full, {
      x: x + 1.5,
      y: y + 0.18,
      w: 2.8,
      h: 0.3,
      fontSize: 10,
      color: C.muted,
      margin: 0,
    });
    s.addText(c.desc, {
      x: x + 0.15,
      y: y + 0.62,
      w: 4.2,
      h: 0.45,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
      lineSpacingMultiple: 1.3,
    });
  });
}

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(s, C.teal, "ETL & Pipelines", "Session 4 · Structure du DAG anime");
  addSlideTitle(
    s,
    "Notre DAG — anime_data_pipeline",
    "5 tâches séquentielles avec gestion d'erreurs intégrée",
  );

  // Pipeline visuel
  const tasks = [
    { id: "audit_data", label: "Audit", color: C.amber },
    { id: "clean_data", label: "Clean", color: C.green },
    { id: "enrich_data", label: "Enrich", color: C.teal },
    { id: "export_json", label: "Export", color: C.purple },
    { id: "validate", label: "Validate", color: C.coral },
  ];

  const boxW = 1.5,
    boxH = 0.55,
    startX = 0.35,
    gapX = 1.8,
    y = 2.05;

  tasks.forEach((t, i) => {
    const x = startX + i * gapX;
    // Box
    s.addShape(pres.shapes.RECTANGLE, {
      x,
      y,
      w: boxW,
      h: boxH,
      fill: { color: C.dark2 },
      line: { color: t.color, width: 1.5 },
    });
    s.addText(t.label, {
      x,
      y: y + 0.08,
      w: boxW,
      h: 0.25,
      fontSize: 12,
      bold: true,
      color: t.color,
      align: "center",
      margin: 0,
    });
    s.addText(t.id, {
      x,
      y: y + 0.32,
      w: boxW,
      h: 0.18,
      fontSize: 7,
      color: C.muted2,
      align: "center",
      margin: 0,
      fontFace: "Consolas",
    });
    // Flèche
    if (i < tasks.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: x + boxW,
        y: y + boxH / 2,
        w: gapX - boxW,
        h: 0,
        line: { color: C.muted, width: 1.5 },
      });
      s.addText("▶", {
        x: x + gapX - 0.15,
        y: y + boxH / 2 - 0.1,
        w: 0.15,
        h: 0.2,
        fontSize: 8,
        color: C.muted,
        margin: 0,
      });
    }
  });

  // Config bloc
  addCard(s, 0.35, 2.9, 4.6, 2.45, C.teal);
  s.addText(
    [
      { text: "default_args", options: { color: C.teal } },
      { text: " = {\n", options: { color: C.white } },
      { text: '  "retries": ', options: { color: C.muted } },
      { text: "2,\n", options: { color: C.amber } },
      { text: '  "retry_delay": ', options: { color: C.muted } },
      { text: "timedelta(minutes=5),\n", options: { color: C.green } },
      { text: '  "email_on_failure": ', options: { color: C.muted } },
      { text: "True,\n", options: { color: C.teal } },
      { text: '  "schedule_interval": ', options: { color: C.muted } },
      { text: '"@weekly"\n', options: { color: C.amber } },
      { text: "}", options: { color: C.white } },
    ],
    {
      x: 0.55,
      y: 3.05,
      w: 4.2,
      h: 2.15,
      fontFace: "Consolas",
      fontSize: 10.5,
      margin: 0,
      lineSpacingMultiple: 1.6,
    },
  );

  // XCom info
  addCard(s, 5.15, 2.9, 4.5, 2.45, C.purple);
  s.addText("Passage de données via XCom", {
    x: 5.3,
    y: 3.05,
    w: 4.1,
    h: 0.3,
    fontSize: 11,
    bold: true,
    color: C.purple,
    margin: 0,
  });
  const xcomItems = [
    "audit_report → clean → enrich",
    "clean_counts → monitoring",
    "export_manifest → validate",
    "validation_status → alerting",
  ];
  xcomItems.forEach((item, i) => {
    s.addText(
      [
        { text: "▸ ", options: { color: C.purple } },
        { text: item, options: { color: C.muted } },
      ],
      {
        x: 5.3,
        y: 3.45 + i * 0.46,
        w: 4.1,
        h: 0.35,
        fontSize: 9.5,
        margin: 0,
      },
    );
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 5 — PIPELINE EXTRACT
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  5,
  "Pipeline Extract :\nLecture Multi-fichiers & Validation",
  "Chargement robuste des sources CSV avec contrôles d'intégrité",
  "Mercredi 25/03 — PM",
  "ETL & Pipelines",
  C.teal,
  "050F10",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.teal,
    "ETL & Pipelines",
    "Session 5 · Mercredi 25/03 PM · 13h30–17h00",
  );
  addSlideTitle(
    s,
    "Stratégie Extract — 3 étapes",
    "Vérification → Chargement → Métriques qualité",
  );

  const steps = [
    {
      n: "01",
      title: "Vérification d'existence",
      color: C.amber,
      code: 'if not path.exists():\n    raise FileNotFoundError(\n        f"Manquant: {path}")',
      desc: "Contrôle avant lecture. Lève une exception immédiate si un fichier source manque — les retries Airflow prennent le relai.",
    },
    {
      n: "02",
      title: "Chargement & métriques",
      color: C.teal,
      code: "df = pd.read_csv(path, low_memory=False)\nnull_pct = df.isnull().mean() * 100\ndup_count = df.duplicated().sum()",
      desc: "Chargement Pandas avec profiling automatique. Chaque CSV produit un rapport de qualité poussé en XCom.",
    },
    {
      n: "03",
      title: "Alertes non-bloquantes",
      color: C.green,
      code: 'if null_pct > 50:\n    alerts.append(f"{col}: {pct}%")\nti.xcom_push("alerts", alerts)',
      desc: "Les anomalies sont loggées et transmises via XCom mais ne bloquent pas le pipeline. Seuils configurables.",
    },
  ];

  steps.forEach((step, i) => {
    const y = 1.55 + i * 1.33;
    addCard(s, 0.35, y, 9.3, 1.18, step.color);
    s.addText(step.n, {
      x: 0.55,
      y: y + 0.25,
      w: 0.55,
      h: 0.55,
      fontSize: 22,
      bold: true,
      color: step.color,
      margin: 0,
      align: "center",
    });
    s.addText(step.title, {
      x: 1.2,
      y: y + 0.1,
      w: 3,
      h: 0.32,
      fontSize: 12,
      bold: true,
      color: C.white,
      margin: 0,
    });
    s.addText(step.desc, {
      x: 1.2,
      y: y + 0.5,
      w: 3,
      h: 0.58,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
      lineSpacingMultiple: 1.3,
    });
    s.addText(step.code, {
      x: 4.4,
      y: y + 0.12,
      w: 5.1,
      h: 0.92,
      fontFace: "Consolas",
      fontSize: 8.5,
      color: C.teal3,
      margin: 0,
      lineSpacingMultiple: 1.55,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 6 — PIPELINE TRANSFORM
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  6,
  "Pipeline Transform :\nNettoyage & Feature Engineering Auto",
  "Automatisation des transformations dans le DAG Airflow",
  "Jeudi 26/03 — AM",
  "ETL & Pipelines",
  C.teal2,
  "05090F",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.teal2,
    "ETL & Pipelines",
    "Session 6 · Jeudi 26/03 AM · 09h00–12h30",
  );
  addSlideTitle(
    s,
    "Transformations automatisées",
    "Du Parquet brut au dataset enrichi prêt à indexer",
  );

  // Flux
  const flow = [
    { label: "Parquet\nbrut", color: C.muted, w: 1.1, h: 0.65 },
    { label: "Clean\ntask", color: C.green, w: 1.3, h: 0.65 },
    { label: "Enrich\ntask", color: C.teal, w: 1.3, h: 0.65 },
    { label: "Parquet\nenrichi", color: C.teal2, w: 1.2, h: 0.65 },
  ];
  let fx = 0.4,
    fy = 1.45;
  flow.forEach((f, i) => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: fx,
      y: fy,
      w: f.w,
      h: f.h,
      fill: { color: C.dark2 },
      line: { color: f.color, width: 1.5 },
    });
    s.addText(f.label, {
      x: fx,
      y: fy + 0.1,
      w: f.w,
      h: f.h - 0.1,
      fontSize: 9.5,
      color: f.color,
      align: "center",
      margin: 0,
      bold: true,
    });
    if (i < flow.length - 1) {
      s.addShape(pres.shapes.LINE, {
        x: fx + f.w,
        y: fy + f.h / 2,
        w: 0.4,
        h: 0,
        line: { color: C.muted, width: 1 },
      });
      fx += f.w + 0.4;
    } else {
      fx += f.w;
    }
  });

  // Transformations détaillées
  const transforms = [
    {
      phase: "clean_data",
      color: C.green,
      items: [
        "anime.csv : dédup + strip noms + genres → liste + typage numeric",
        "synopsis.csv : rename MAL_ID, nettoyage texte vide",
        "rating.csv : suppression -1, clip[1-10], dédup user×anime",
        "Sauvegarde en Parquet (préserve les listes Python)",
      ],
    },
    {
      phase: "enrich_data",
      color: C.teal,
      items: [
        "Jointure anime ← synopsis (left merge sur anime_id)",
        "Agrégation ratings : mean / count / std par anime",
        "Calcul quality_score (formule composite pondérée)",
        "popularity_tier, tags, has_synopsis → nouvelles colonnes",
      ],
    },
  ];

  transforms.forEach((t, i) => {
    const y = 2.45 + i * 1.5;
    addCard(s, 0.35, y, 9.3, 1.32, t.color);
    s.addText(t.phase + "()", {
      x: 0.55,
      y: y + 0.12,
      w: 2.5,
      h: 0.3,
      fontSize: 11,
      bold: true,
      color: t.color,
      fontFace: "Consolas",
      margin: 0,
    });
    const parts = [];
    t.items.forEach((item, j) => {
      parts.push({ text: "→ ", options: { color: t.color, bold: true } });
      parts.push({
        text: item,
        options: { color: C.muted, breakLine: j < t.items.length - 1 },
      });
    });
    s.addText(parts, {
      x: 3.15,
      y: y + 0.1,
      w: 6.35,
      h: 1.1,
      fontSize: 9.5,
      margin: 0,
      lineSpacingMultiple: 1.45,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SESSION 7 — PIPELINE LOAD
// ═══════════════════════════════════════════════════════════════════════════════
makeSectionSlide(
  7,
  "Pipeline Load :\nIndexation Elasticsearch & Monitoring",
  "Export JSON, indexation et validation de bout en bout",
  "Jeudi 26/03 — PM",
  "ETL & Pipelines",
  C.coral,
  "10050A",
);

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.coral,
    "ETL & Pipelines",
    "Session 7 · Jeudi 26/03 PM · 13h30–17h00",
  );
  addSlideTitle(
    s,
    "Export JSON — 3 fichiers + manifeste",
    "Chaque fichier répond à un usage précis",
  );

  const exports = [
    {
      file: "anime_full.json",
      size: "~18 MB",
      desc: "Dataset complet avec timestamp. Import BDD, API REST, batch processing.",
      color: C.teal,
    },
    {
      file: "anime_top100.json",
      size: "~150 KB",
      desc: "Top 100 par quality_score. Page d'accueil, widgets, recommandations rapides.",
      color: C.amber,
    },
    {
      file: "anime_by_genre.json",
      size: "~2 MB",
      desc: "Index genre → liste d'anime triée. Filtres, navigation, recommandations par catégorie.",
      color: C.purple,
    },
    {
      file: "export_manifest.json",
      size: "~2 KB",
      desc: "Tailles de fichiers, stats clés, run_id Airflow. Monitoring et audit pipeline.",
      color: C.coral,
    },
  ];

  exports.forEach((e, i) => {
    const y = 1.5 + i * 1.01;
    addCard(s, 0.35, y, 9.3, 0.88, e.color);
    s.addText(e.file, {
      x: 0.55,
      y: y + 0.1,
      w: 3.5,
      h: 0.3,
      fontSize: 11,
      bold: true,
      color: e.color,
      fontFace: "Consolas",
      margin: 0,
    });
    s.addText(e.size, {
      x: 0.55,
      y: y + 0.52,
      w: 3.5,
      h: 0.28,
      fontSize: 10,
      color: C.muted2,
      margin: 0,
      italic: true,
    });
    s.addText(e.desc, {
      x: 4.2,
      y: y + 0.22,
      w: 5.25,
      h: 0.45,
      fontSize: 10,
      color: C.muted,
      margin: 0,
      lineSpacingMultiple: 1.35,
    });
  });
}

{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);
  addTopBar(
    s,
    C.coral,
    "ETL & Pipelines",
    "Session 7 · Monitoring & validation",
  );
  addSlideTitle(
    s,
    "Validation post-export & Monitoring",
    "3 niveaux de contrôle qualité de bout en bout",
  );

  const levels = [
    {
      level: "Niveau 1",
      title: "Retries automatiques Airflow",
      color: C.teal,
      items: [
        "2 retries avec délai de 5 min",
        "Email sur failure",
        "Logs centralisés dans l'UI Airflow",
      ],
    },
    {
      level: "Niveau 2",
      title: "Exceptions métier explicites",
      color: C.amber,
      items: [
        "FileNotFoundError si CSV source manquant",
        "ValueError si fichier JSON trop petit (<100 octets)",
        "JSONDecodeError si sérialisation corrompue",
      ],
    },
    {
      level: "Niveau 3",
      title: "Tâche validate_output",
      color: C.coral,
      items: [
        "Vérifie existence et intégrité de tous les exports",
        "Contrôle logique : anime_top100 = exactement 100 entrées",
        "Rapport XCom : validation_status = 'passed'",
      ],
    },
  ];

  levels.forEach((l, i) => {
    const y = 1.5 + i * 1.35;
    addCard(s, 0.35, y, 9.3, 1.22, l.color);
    s.addText(l.level.toUpperCase(), {
      x: 0.55,
      y: y + 0.1,
      w: 1.5,
      h: 0.22,
      fontSize: 7.5,
      bold: true,
      color: l.color,
      charSpacing: 2,
      margin: 0,
    });
    s.addText(l.title, {
      x: 0.55,
      y: y + 0.35,
      w: 2.6,
      h: 0.32,
      fontSize: 12,
      bold: true,
      color: C.white,
      margin: 0,
    });
    const parts = [];
    l.items.forEach((item, j) => {
      parts.push({ text: "✓  ", options: { color: l.color, bold: true } });
      parts.push({
        text: item,
        options: { color: C.muted, breakLine: j < l.items.length - 1 },
      });
    });
    s.addText(parts, {
      x: 3.35,
      y: y + 0.1,
      w: 6.1,
      h: 1.02,
      fontSize: 10.5,
      margin: 0,
      lineSpacingMultiple: 1.55,
    });
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLIDE FINALE — RÉCAP
// ═══════════════════════════════════════════════════════════════════════════════
{
  const s = pres.addSlide();
  addDarkBg(s, C.dark);

  s.addShape(pres.shapes.OVAL, {
    x: -1,
    y: 3,
    w: 5,
    h: 5,
    fill: { color: C.teal, transparency: 90 },
    line: { color: C.teal, transparency: 90 },
  });

  s.addText("RÉCAPITULATIF", {
    x: 0.6,
    y: 0.6,
    w: 5,
    h: 0.35,
    fontSize: 8,
    bold: true,
    color: C.teal,
    charSpacing: 4,
    margin: 0,
  });

  s.addText("Ce qu'on a construit\nensemble", {
    x: 0.6,
    y: 1.0,
    w: 8.5,
    h: 1.2,
    fontSize: 34,
    bold: true,
    color: C.white,
    margin: 0,
    lineSpacingMultiple: 1.15,
  });

  const recap = [
    {
      icon: "📊",
      label: "Audit complet",
      sub: "Qualité, doublons, cohérence",
      color: C.amber,
    },
    {
      icon: "🔧",
      label: "Nettoyage robuste",
      sub: "3 CSV → Parquet normalisés",
      color: C.green,
    },
    {
      icon: "✨",
      label: "Feature Engineering",
      sub: "quality_score, tiers, tags",
      color: C.teal,
    },
    {
      icon: "🔍",
      label: "ELK Stack",
      sub: "Index Elasticsearch + Kibana",
      color: C.purple,
    },
    {
      icon: "⚙️",
      label: "DAG Airflow",
      sub: "5 tâches + retry + monitoring",
      color: C.teal2,
    },
    {
      icon: "✅",
      label: "Validation end-to-end",
      sub: "3 niveaux de contrôle qualité",
      color: C.coral,
    },
  ];

  recap.forEach((r, i) => {
    const col = i % 3;
    const row = Math.floor(i / 3);
    const x = 0.4 + col * 3.18;
    const y = 2.55 + row * 1.35;
    addCard(s, x, y, 2.98, 1.15, r.color);
    s.addText(r.label, {
      x: x + 0.15,
      y: y + 0.15,
      w: 2.7,
      h: 0.35,
      fontSize: 12.5,
      bold: true,
      color: r.color,
      margin: 0,
    });
    s.addText(r.sub, {
      x: x + 0.15,
      y: y + 0.58,
      w: 2.7,
      h: 0.45,
      fontSize: 9.5,
      color: C.muted,
      margin: 0,
      lineSpacingMultiple: 1.3,
    });
  });

  // Barre finale
  const barColors = [
    C.amber,
    C.green,
    C.purple,
    C.teal,
    C.teal,
    C.teal2,
    C.coral,
  ];
  barColors.forEach((c, i) => {
    s.addShape(pres.shapes.RECTANGLE, {
      x: i * (10 / 7),
      y: 5.555,
      w: 10 / 7,
      h: 0.07,
      fill: { color: c },
      line: { color: c },
    });
  });
}

// ── Save ────────────────────────────────────────────────────────────────────
pres
  .writeFile({ fileName: "Anime_Data_Pipeline_Presentation.pptx" })
  .then(() => console.log("✓ PPTX généré"))
  .catch((e) => {
    console.error(e);
    process.exit(1);
  });
