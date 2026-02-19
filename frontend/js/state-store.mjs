export const createStateStore = (initialState = {}) => {
  const state = { ...initialState };

  return {
    get(key) {
      return state[key];
    },
    set(key, value) {
      state[key] = value;
      return value;
    },
    update(key, updater) {
      const nextValue = updater(state[key]);
      state[key] = nextValue;
      return nextValue;
    },
    snapshot() {
      return { ...state };
    },
  };
};

export const setCookieValue = (name, value, maxAgeSeconds) => {
  document.cookie = [
    `${name}=${encodeURIComponent(value)}`,
    "Path=/",
    "SameSite=Lax",
    `Max-Age=${maxAgeSeconds}`,
  ].join("; ");
};

export const getCookieValue = (name) => {
  const cookiePrefix = `${name}=`;
  const cookieValue = document.cookie
    .split(";")
    .map((item) => item.trim())
    .find((item) => item.startsWith(cookiePrefix));

  if (!cookieValue) {
    return null;
  }

  return decodeURIComponent(cookieValue.slice(cookiePrefix.length));
};

export const clearCookieValue = (name) => {
  document.cookie = `${name}=; Path=/; SameSite=Lax; Max-Age=0`;
};
