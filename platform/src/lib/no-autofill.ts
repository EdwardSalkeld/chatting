// Search boxes get offered saved logins by browsers and password managers
// (1Password, LastPass, Bitwarden, Dashlane) unless each is told to skip them.
export const noAutofill = {
  autoComplete: 'off',
  autoCorrect: 'off',
  spellCheck: false,
  'data-1p-ignore': true,
  'data-lpignore': 'true',
  'data-bwignore': 'true',
  'data-form-type': 'other',
} as const
