CREATE TYPE n_state_mtm AS (
    min numeric,
    max numeric,
    cntr numeric -- actually numeric here net necessary
);

CREATE OR REPLACE FUNCTION n_transition_mtm(n_state_mtm, numeric) RETURNS n_state_mtm
    AS 'mtm', 'n_transition_mtm'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION n_final_mtm(n_state_mtm) RETURNS text
    AS 'mtm', 'n_final_mtm'
    LANGUAGE C STRICT;

CREATE OR REPLACE AGGREGATE max_to_min(numeric) (
    stype     = n_state_mtm,
    initcond  = '(0,0,0)',
    sfunc     = n_transition_mtm,
    finalfunc = n_final_mtm
);
