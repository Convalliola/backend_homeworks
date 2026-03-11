CREATE TABLE IF NOT EXISTS public.account (
    id         SERIAL PRIMARY KEY,
    login      TEXT NOT NULL UNIQUE,
    password   TEXT NOT NULL,
    is_blocked BOOLEAN NOT NULL DEFAULT FALSE
);
