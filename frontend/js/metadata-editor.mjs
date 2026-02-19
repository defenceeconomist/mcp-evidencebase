const DEFAULT_SCHEMA = {
  document_types: [
    "article",
    "book",
    "booklet",
    "conference",
    "inbook",
    "incollection",
    "inproceedings",
    "manual",
    "mastersthesis",
    "misc",
    "phdthesis",
    "proceedings",
    "techreport",
    "unpublished",
  ],
  bibtex_fields: [
    "address",
    "annote",
    "author",
    "booktitle",
    "chapter",
    "crossref",
    "doi",
    "isbn",
    "issn",
    "edition",
    "editor",
    "email",
    "howpublished",
    "institution",
    "journal",
    "month",
    "note",
    "number",
    "organization",
    "pages",
    "publisher",
    "school",
    "series",
    "title",
    "type",
    "volume",
    "year",
  ],
  bibtex_type_rules: {
    article: {
      required: ["author", "title", "journal", "year"],
      recommended: ["volume", "number", "pages", "month", "note", "doi", "issn"],
    },
    book: {
      required: ["title", "author", "year", "publisher", "address"],
      recommended: ["editor", "volume", "number", "series", "edition", "month", "note", "doi", "isbn"],
    },
    booklet: {
      required: ["title"],
      recommended: ["author", "howpublished", "address", "month", "year", "note", "isbn"],
    },
    conference: {
      required: ["author", "title", "booktitle", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "pages",
        "address",
        "month",
        "organization",
        "publisher",
        "note",
      ],
    },
    inbook: {
      required: ["author", "title", "booktitle", "publisher", "year"],
      recommended: [
        "editor",
        "chapter",
        "pages",
        "volume",
        "number",
        "series",
        "address",
        "edition",
        "month",
        "note",
        "isbn",
      ],
    },
    incollection: {
      required: ["author", "title", "booktitle", "publisher", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "chapter",
        "pages",
        "address",
        "edition",
        "month",
        "organization",
        "note",
        "isbn",
      ],
    },
    inproceedings: {
      required: ["author", "title", "booktitle", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "pages",
        "address",
        "month",
        "organization",
        "publisher",
        "note",
      ],
    },
    manual: {
      required: ["title"],
      recommended: ["author", "organization", "address", "edition", "month", "year", "note", "isbn"],
    },
    mastersthesis: {
      required: ["author", "title", "school", "year"],
      recommended: ["type", "address", "month", "note"],
    },
    misc: {
      required: [],
      recommended: ["author", "title", "howpublished", "month", "year", "note"],
    },
    phdthesis: {
      required: ["author", "title", "school", "year"],
      recommended: ["type", "address", "month", "note"],
    },
    proceedings: {
      required: ["title", "year"],
      recommended: [
        "editor",
        "volume",
        "number",
        "series",
        "address",
        "month",
        "publisher",
        "organization",
        "note",
        "isbn",
      ],
    },
    techreport: {
      required: ["author", "title", "institution", "year"],
      recommended: ["type", "number", "address", "month", "note"],
    },
    unpublished: {
      required: ["author", "title", "note"],
      recommended: ["month", "year"],
    },
  },
  known_author_suffixes: ["jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v", "phd", "m.d.", "md"],
  crossref_lookup_seed_fields: ["doi", "issn", "isbn", "title"],
  minimal_metadata_identity_fields: ["doi", "issn", "isbn", "title", "author"],
};

export const normalizeText = (value) => {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
};

export const normalizeCitationSchema = (schema) => {
  const source = schema && typeof schema === "object" ? schema : {};
  return {
    document_types: Array.isArray(source.document_types)
      ? source.document_types.map((value) => normalizeText(value).toLowerCase()).filter(Boolean)
      : [...DEFAULT_SCHEMA.document_types],
    bibtex_fields: Array.isArray(source.bibtex_fields)
      ? source.bibtex_fields.map((value) => normalizeText(value).toLowerCase()).filter(Boolean)
      : [...DEFAULT_SCHEMA.bibtex_fields],
    bibtex_type_rules:
      source.bibtex_type_rules && typeof source.bibtex_type_rules === "object"
        ? source.bibtex_type_rules
        : { ...DEFAULT_SCHEMA.bibtex_type_rules },
    known_author_suffixes: Array.isArray(source.known_author_suffixes)
      ? source.known_author_suffixes.map((value) => normalizeText(value).toLowerCase()).filter(Boolean)
      : [...DEFAULT_SCHEMA.known_author_suffixes],
    crossref_lookup_seed_fields: Array.isArray(source.crossref_lookup_seed_fields)
      ? source.crossref_lookup_seed_fields.map((value) => normalizeText(value).toLowerCase()).filter(Boolean)
      : [...DEFAULT_SCHEMA.crossref_lookup_seed_fields],
    minimal_metadata_identity_fields: Array.isArray(source.minimal_metadata_identity_fields)
      ? source.minimal_metadata_identity_fields
          .map((value) => normalizeText(value).toLowerCase())
          .filter(Boolean)
      : [...DEFAULT_SCHEMA.minimal_metadata_identity_fields],
  };
};

export const getDefaultCitationSchema = () => normalizeCitationSchema(DEFAULT_SCHEMA);

export const getBulkBibtexFieldOrder = (bibtexFields, bibtexTypeRules) => {
  const metrics = new Map();
  bibtexFields.forEach((fieldName) => {
    metrics.set(fieldName, { requiredCount: 0, recommendedCount: 0 });
  });
  Object.values(bibtexTypeRules || {}).forEach((rules) => {
    (rules.required || []).forEach((fieldName) => {
      if (metrics.has(fieldName)) {
        metrics.get(fieldName).requiredCount += 1;
      }
    });
    (rules.recommended || []).forEach((fieldName) => {
      if (metrics.has(fieldName)) {
        metrics.get(fieldName).recommendedCount += 1;
      }
    });
  });
  return [...bibtexFields].sort((leftField, rightField) => {
    const left = metrics.get(leftField);
    const right = metrics.get(rightField);
    if (left.requiredCount !== right.requiredCount) {
      return right.requiredCount - left.requiredCount;
    }
    if (left.recommendedCount !== right.recommendedCount) {
      return right.recommendedCount - left.recommendedCount;
    }
    return leftField.localeCompare(rightField);
  });
};

export const getBibtexFieldStatus = ({ documentType, fieldName, bibtexTypeRules }) => {
  const rules = bibtexTypeRules[normalizeDocumentType(documentType, Object.keys(bibtexTypeRules))] || {
    required: [],
    recommended: [],
  };
  if ((rules.required || []).includes(fieldName)) {
    return "required";
  }
  if ((rules.recommended || []).includes(fieldName)) {
    return "recommended";
  }
  return "optional";
};

export const normalizeDocumentType = (value, documentTypes) => {
  const normalized = normalizeText(value).toLowerCase();
  if (!normalized) {
    return "misc";
  }
  return documentTypes.includes(normalized) ? normalized : "misc";
};

export const stripOuterBraces = (value) => {
  let normalizedValue = normalizeText(value);
  while (
    normalizedValue.startsWith("{") &&
    normalizedValue.endsWith("}") &&
    normalizedValue.length >= 2
  ) {
    normalizedValue = normalizeText(normalizedValue.slice(1, -1));
  }
  return normalizedValue;
};

export const isLikelyAuthorSuffix = (value, knownAuthorSuffixes) => {
  const normalizedValue = normalizeText(value).toLowerCase();
  if (!normalizedValue) {
    return false;
  }
  return knownAuthorSuffixes.has(normalizedValue);
};

export const parseAuthorFromText = (value, knownAuthorSuffixes) => {
  const normalizedValue = stripOuterBraces(value);
  if (!normalizedValue) {
    return null;
  }

  if (normalizedValue.includes(",")) {
    const commaParts = normalizedValue
      .split(",")
      .map((part) => stripOuterBraces(part))
      .filter(Boolean);
    if (commaParts.length >= 3) {
      const lastName = commaParts[0];
      const suffix = commaParts[1];
      const firstName = commaParts.slice(2).join(", ");
      return normalizeAuthorNameParts(firstName, lastName, suffix);
    }
    if (commaParts.length === 2) {
      const lastName = commaParts[0];
      const firstName = commaParts[1];
      return normalizeAuthorNameParts(firstName, lastName, "");
    }
  }

  const tokens = normalizedValue.split(/\s+/).filter(Boolean);
  if (!tokens.length) {
    return null;
  }
  if (tokens.length === 1) {
    return normalizeAuthorNameParts("", tokens[0], "");
  }

  let suffix = "";
  let bodyTokens = [...tokens];
  const trailingToken = tokens[tokens.length - 1];
  if (isLikelyAuthorSuffix(trailingToken, knownAuthorSuffixes)) {
    suffix = trailingToken;
    bodyTokens = tokens.slice(0, -1);
  }
  if (!bodyTokens.length) {
    return normalizeAuthorNameParts("", "", suffix);
  }

  const lastName = bodyTokens[bodyTokens.length - 1];
  const firstName = bodyTokens.slice(0, -1).join(" ");
  return normalizeAuthorNameParts(firstName, lastName, suffix);
};

export const normalizeAuthorNameParts = (firstName, lastName, suffix) => {
  const normalized = {
    first_name: normalizeText(firstName),
    last_name: normalizeText(lastName),
    suffix: normalizeText(suffix),
  };
  if (!normalized.first_name && !normalized.last_name && !normalized.suffix) {
    return null;
  }
  return normalized;
};

export const normalizeAuthorEntries = (rawAuthors, knownAuthorSuffixes) => {
  if (!Array.isArray(rawAuthors)) {
    return [];
  }
  return rawAuthors
    .map((rawAuthor) => normalizeAuthorEntry(rawAuthor, knownAuthorSuffixes))
    .filter((authorEntry) => Boolean(authorEntry));
};

export const normalizeAuthorEntry = (rawAuthor, knownAuthorSuffixes) => {
  if (!rawAuthor) {
    return null;
  }
  if (typeof rawAuthor === "string") {
    return parseAuthorFromText(rawAuthor, knownAuthorSuffixes);
  }
  if (typeof rawAuthor !== "object") {
    return null;
  }

  return normalizeAuthorNameParts(
    rawAuthor.first_name || rawAuthor.firstName || rawAuthor.first || rawAuthor.given || "",
    rawAuthor.last_name || rawAuthor.lastName || rawAuthor.last || rawAuthor.family || "",
    rawAuthor.suffix || rawAuthor.suffix_name || rawAuthor.suf || ""
  );
};

export const getAuthorInitials = (firstName) => {
  return normalizeText(firstName)
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => {
      const firstLetter = token.match(/[A-Za-z]/);
      return firstLetter ? `${firstLetter[0].toUpperCase()}.` : "";
    })
    .filter(Boolean)
    .join(" ");
};

export const formatAuthorHarvard = (authorEntry) => {
  if (!authorEntry || typeof authorEntry !== "object") {
    return "";
  }
  const firstName = normalizeText(authorEntry.first_name);
  const lastName = normalizeText(authorEntry.last_name);
  const suffix = normalizeText(authorEntry.suffix);
  const initials = getAuthorInitials(firstName);

  let formattedName = "";
  if (lastName && initials) {
    formattedName = `${lastName}, ${initials}`;
  } else if (lastName) {
    formattedName = lastName;
  } else if (firstName) {
    formattedName = firstName;
  }
  if (!formattedName) {
    return "";
  }
  if (suffix) {
    return `${formattedName}, ${suffix}`;
  }
  return formattedName;
};

export const formatAuthorsHarvard = (authorEntries, knownAuthorSuffixes) => {
  const normalizedEntries = normalizeAuthorEntries(authorEntries, knownAuthorSuffixes);
  if (!normalizedEntries.length) {
    return "";
  }
  const formattedAuthors = normalizedEntries
    .map((authorEntry) => formatAuthorHarvard(authorEntry))
    .filter(Boolean);
  if (!formattedAuthors.length) {
    return "";
  }
  if (formattedAuthors.length === 1) {
    return formattedAuthors[0];
  }
  if (formattedAuthors.length === 2) {
    return `${formattedAuthors[0]} & ${formattedAuthors[1]}`;
  }
  return `${formattedAuthors.slice(0, -1).join(", ")} & ${formattedAuthors[formattedAuthors.length - 1]}`;
};

export const normalizeCitationToken = (value) => {
  return normalizeText(value).toLowerCase().replace(/[^a-z0-9]+/g, "");
};

export const extractCitationYearToken = (value) => {
  const normalizedValue = normalizeText(value);
  if (!normalizedValue) {
    return "";
  }
  const yearMatch = normalizedValue.match(/(?:19|20)\d{2}/);
  if (yearMatch) {
    return yearMatch[0];
  }
  const fallbackYearMatch = normalizedValue.match(/\d{4}/);
  return fallbackYearMatch ? fallbackYearMatch[0] : "";
};

export const extractCitationFirstTitleWord = (value) => {
  const normalizedValue = stripOuterBraces(value);
  if (!normalizedValue) {
    return "";
  }
  const titleWordMatch = normalizedValue.match(/[A-Za-z0-9]+/);
  return titleWordMatch ? titleWordMatch[0] : "";
};

export const buildFallbackCitationKey = ({
  filePath,
  index,
  authorLastName,
  title,
  year,
  filenameFromPath,
}) => {
  const authorToken = normalizeCitationToken(authorLastName);
  const yearToken = extractCitationYearToken(year);
  const titleToken = normalizeCitationToken(extractCitationFirstTitleWord(title));
  const candidate = `${authorToken}${yearToken}${titleToken}`;
  if (candidate) {
    return candidate;
  }

  const withoutExtension = filenameFromPath(filePath).replace(/\.[^/.]+$/, "");
  const slug = withoutExtension
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+/, "")
    .replace(/-+$/, "");
  return slug || `document-${index + 1}`;
};
