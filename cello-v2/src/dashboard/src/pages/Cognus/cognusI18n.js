import { getLocale } from 'umi';

export const COGNUS_LOCALE_EN_US = 'en-US';
export const COGNUS_LOCALE_PT_BR = 'pt-BR';

export const resolveCognusLocale = localeCandidate => {
  const runtimeLocale = typeof getLocale === 'function' ? getLocale() : COGNUS_LOCALE_EN_US;
  const normalized = String(localeCandidate || runtimeLocale || COGNUS_LOCALE_EN_US)
    .trim()
    .toLowerCase();

  return normalized.startsWith('pt') ? COGNUS_LOCALE_PT_BR : COGNUS_LOCALE_EN_US;
};

export const isCognusPortuguese = localeCandidate =>
  resolveCognusLocale(localeCandidate) === COGNUS_LOCALE_PT_BR;

export const pickCognusText = (ptBR, enUS, localeCandidate) =>
  isCognusPortuguese(localeCandidate) ? ptBR : enUS;

export const formatCognusTemplate = (ptBR, enUS, values, localeCandidate) => {
  const safeValues = values && typeof values === 'object' ? values : {};
  const template = pickCognusText(ptBR, enUS, localeCandidate);
  return Object.entries(safeValues).reduce((currentText, [key, value]) => {
    return currentText.replace(new RegExp(`\\{${key}\\}`, 'g'), String(value));
  }, template);
};

export const formatCognusDateTime = (value, localeCandidate, options = {}) => {
  const safeValue = String(value || '').trim();
  if (!safeValue) {
    return '';
  }

  const parsedDate = new Date(safeValue);
  if (Number.isNaN(parsedDate.getTime())) {
    return safeValue;
  }

  return parsedDate.toLocaleString(resolveCognusLocale(localeCandidate), options);
};
