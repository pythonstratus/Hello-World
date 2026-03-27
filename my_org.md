**`ENTITYDEV.MYORG`** — Returns the 2-character org code for the current database session user by looking up their Unix username in `ENTEMP` (where `EACTIVE = 'A'`). If the user has exactly one active record, it returns that org; otherwise (zero or multiple matches, or any exception), it returns `'XX'`.

Key caveat: In the Java Spring connection pool, `osuser` from `v$session` resolves to the app server's OS user — not the actual end user — so this function will always return `'XX'` unless replaced with a SWITCHROID-based approach.
