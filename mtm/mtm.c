#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"  /* GetAttributeByNum() */
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
    values[i - 1] = GetAttributeByNum(t, i, &isnull);
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

PG_FUNCTION_INFO_V1(f_transition_mtm);

Datum
f_transition_mtm(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;
    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    HeapTuple tuple;
    double tbl_val = PG_GETARG_FLOAT8(1); 
    bool isnull;
    Datum values[3] = {0, 0, 0}; 
    Datum result;
    bool isnullarr[3] = {false, false, false}; 


    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    for (int i = 1; i <= 3; i++) {
        values[i - 1] = GetAttributeByNum(t, i, &isnull);
        if (isnull) {
            isnullarr[i - 1] = true;
        }
    }


    if (!PG_ARGISNULL(1)) {

        if (DatumGetFloat8(values[2]) != 0) {

            if (!isnullarr[0]) {
                if (DatumGetFloat8(values[0]) > tbl_val) {
                    values[0] = Float8GetDatum(tbl_val); 
                }
            } else {

                values[0] = Float8GetDatum(tbl_val);
                isnullarr[0] = false;
            }


            if (!isnullarr[1]) {
                if (DatumGetFloat8(values[1]) < tbl_val) {
                    values[1] = Float8GetDatum(tbl_val); 
                }
            } else {

                values[1] = Float8GetDatum(tbl_val);
                isnullarr[1] = false;
            }
        } else if (!isnullarr[2]) {

            values[0] = Float8GetDatum(tbl_val);
            values[1] = Float8GetDatum(tbl_val);
            values[2] = Float8GetDatum(DatumGetFloat8(values[2]) + 1); 
        }
    }

    /* Обработка результата кортежа */
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
    char outp[256];
    const char *work_mem_str, *num_str1, *num_str2;

    for (int i = 1; i <= 3; i++) {
      values[i - 1] = GetAttributeByNum(t, i, &isnull);
    }

    if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[2], NumericGetDatum(int64_to_numeric(0)))) == 0) {
      PG_RETURN_NULL();
    }

    work_mem_str = GetConfigOption("mtm.output_format", true, false) ?: "%s --> %s";
    
    num_str1 = numeric_normalize(DatumGetNumeric(values[0]));
    num_str2 = numeric_normalize(DatumGetNumeric(values[1]));
    
   
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

PG_FUNCTION_INFO_V1(n_final_mtm);

Datum
f_final_mtm(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;

    /* Проверка типа результата функции */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning scalar called in context "
                        "that cannot accept type scalar")));

    HeapTupleHeader t = PG_GETARG_HEAPTUPLEHEADER(0);
    bool isnull;
    Datum values[3];
    char outp[256];
    const char *work_mem_str, *num_str1, *num_str2;

    /* Извлечение атрибутов из кортежа */
    for (int i = 1; i <= 3; i++) {
        values[i - 1] = GetAttributeByNum(t, i, &isnull);
    }

    /* Проверка: если счётчик равен 0, возвращаем NULL */
    if (DatumGetFloat8(values[2]) == 0) {
        PG_RETURN_NULL();
    }

    /* Получаем строку формата вывода из конфигурации */
    work_mem_str = GetConfigOption("mtm.output_format", true, false) ?: "%s --> %s";

    /* Преобразуем значения double precision в строки */
    num_str1 = psprintf("%lf", DatumGetFloat8(values[0]));
    num_str2 = psprintf("%lf", DatumGetFloat8(values[1]));

    /* Проверка на успешное преобразование и формирование выходной строки */
    if (num_str1 != NULL && num_str2 != NULL) {
        int res = snprintf(outp, sizeof(outp), work_mem_str, num_str1, num_str2);
        if (res < 0 || res >= sizeof(outp)) {
            ereport(ERROR,
                    (errmsg("Error formatting output or buffer too small")));
        }
    } else {
        ereport(ERROR,
                (errmsg("Double precision to string conversion failed")));
    }

    PG_RETURN_TEXT_P(cstring_to_text(outp));
}
