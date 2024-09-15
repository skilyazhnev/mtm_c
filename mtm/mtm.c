#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"  /* GetAttributeByName() */
#include "funcapi.h"
#include "utils/guc.h" /* GetConfigOption */
#include "utils/builtins.h" /* text stuff */
#include "utils/numeric.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(n_transition_mtm);

Datum
n_transition_mtm(PG_FUNCTION_ARGS) {

  TupleDesc tupdesc;
  HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
  HeapTuple tuple;
  Numeric tbl_val = PG_GETARG_NUMERIC(1);
  bool isnull;
  Datum values[3] = {0,0,0};
  Datum result;
  bool isnullarr[3] = {0,0,0};

  if (get_call_result_type(fcinfo, NULL, & tupdesc) != TYPEFUNC_COMPOSITE)
    ereport(ERROR,
      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("function returning record called in context "
          "that cannot accept type record")));

  for (int i = 1; i <= 3; i++) {
    values[i - 1] = GetAttributeByNum(t, i, & isnull);
    if (isnull)
      isnullarr[i - 1] = isnull;
  }

  if (!PG_ARGISNULL(1) && !numeric_is_nan(tbl_val)) {
    if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[2], NumericGetDatum(int64_to_numeric(0)))) != 0) {
      if (!isnullarr[0]) {
        if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[0], NumericGetDatum(tbl_val))) > 0) {
          values[0] = NumericGetDatum(tbl_val);
        }
      }
      if (!isnullarr[1]) {
        if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[1], NumericGetDatum(tbl_val))) < 0) {
          values[1] = NumericGetDatum(tbl_val);
        }
      }
    } else if (!isnullarr[2] ) {
      values[0] = NumericGetDatum(tbl_val);
      values[1] = NumericGetDatum(tbl_val);
      values[2] = NumericGetDatum(DirectFunctionCall2(numeric_add, values[2], NumericGetDatum(int64_to_numeric(1))));
    }
  }

  tupdesc = BlessTupleDesc(tupdesc);

  tuple = heap_form_tuple(tupdesc, values, isnullarr);

  result = HeapTupleGetDatum(tuple);

  PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(n_final_mtm);

Datum
n_final_mtm(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;

    if (get_call_result_type(fcinfo, NULL, & tupdesc) != TYPEFUNC_SCALAR)
      ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
          errmsg("function returning record called in context "
            "that cannot accept type record")));

    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    bool isnull;
    Datum values[3];
    bool isnullarr[3] = {0,0,0};
    HeapTuple tuple;

    for (int i = 1; i <= 3; i++) {
      values[i - 1] = GetAttributeByNum(t, i, & isnull);
      if (isnull)
        isnullarr[i - 1] = isnull;
    }

    if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[2], NumericGetDatum(int64_to_numeric(0)))) == 0) {
      PG_RETURN_NULL();
    }

    char outp[256];
    const char *work_mem_str;
    
    work_mem_str = GetConfigOption("mtm.output_format", true, false) ?: "%s --> %s";
    
    const char *num_str1 = numeric_normalize(DatumGetNumeric(values[0]));
    const char *num_str2 = numeric_normalize(DatumGetNumeric(values[1]));
    
    // Безопасное форматирование строки с проверкой возвращаемого значения
    if (num_str1 != NULL && num_str2 != NULL) {
        int res = snprintf(outp, sizeof(outp), work_mem_str, num_str1, num_str2);
        if (res < 0 || res >= sizeof(outp)) {
            ereport(ERROR,
                    (errmsg("Error formatting output or buffer too small")));
        }
    } else {
        ereport(ERROR,
                (errmsg("Numeric normalization failed")));
    }
    PG_RETURN_TEXT_P(cstring_to_text(outp));
}
