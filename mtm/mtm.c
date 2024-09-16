#include "postgres.h"
#include "fmgr.h"
#include "executor/executor.h"  /* GetAttributeByNum() */
#include "funcapi.h"
#include "utils/guc.h" /* GetConfigOption */
#include "utils/builtins.h" /* text stuff */
#include "utils/numeric.h"
#include <string.h> /* trim func */

/* Define constants for maximum number of arguments and default output format */
#define MTM_MAX_ARGS_COUNT 2
#define MTM_DEFAULT_OUTPUT_FORMAT "%s -> %s"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void float_trim_zeros(char *str);
char* mtm_dynamic_sprintf(const char *format, ...);
int count_format_specifiers(const char *format);

PG_FUNCTION_INFO_V1(n_transition_mtm);

/* Function for numeric transition (for numeric data type) */
Datum
n_transition_mtm(PG_FUNCTION_ARGS) {

  TupleDesc tupdesc;
  HeapTupleHeader t;
  HeapTuple tuple;
  Numeric tbl_val;
  bool isnull;
  Datum values[3] = {0,0,0}; /* Array to store attribute values */
  Datum result;
  bool isnullarr[3] = {0,0,0}; /* Array to track null values */

  /* Extract the tuple and numeric value from function arguments */
  t = PG_GETARG_HEAPTUPLEHEADER(0);
  tbl_val = PG_GETARG_NUMERIC(1);

  /* Verify that the return type is a composite type */
  if (get_call_result_type(fcinfo, NULL, & tupdesc) != TYPEFUNC_COMPOSITE)
    ereport(ERROR,
      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
        errmsg("function returning record called in context "
          "that cannot accept type record")));

  /* Extract attributes from the tuple and check for nulls */
  for (int i = 1; i <= 3; i++) {
    values[i - 1] = GetAttributeByNum(t, i, &isnull);
    if (isnull)
      isnullarr[i - 1] = isnull;
  }

  /* Check if the numeric value is not null and not NaN */
  if (!PG_ARGISNULL(1) && !numeric_is_nan(tbl_val)) {
    /* Compare numeric value with current table value and update if necessary.
       I didn't check performance of DirectFunctionCall2 but just 
       in case we have float8 realisation too
     */
    if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[2], NumericGetDatum(int64_to_numeric(0)))) != 0) {
      if (!isnullarr[0]) { /*we can merge two ifs, but too spaghetti in one line*/
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
      /* If values[2] (cntr) is zero (first call), update all values and increment the count */
      values[0] = NumericGetDatum(tbl_val);
      values[1] = NumericGetDatum(tbl_val);
      values[2] = NumericGetDatum(DirectFunctionCall2(numeric_add, values[2], NumericGetDatum(int64_to_numeric(1))));
    }
  }

  /* Create a new tuple with the updated values */
  tupdesc = BlessTupleDesc(tupdesc);
  tuple = heap_form_tuple(tupdesc, values, isnullarr);
  result = HeapTupleGetDatum(tuple);

  PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(f_transition_mtm);

/* Function for float transition (for double precision data type) */
Datum
f_transition_mtm(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;
    HeapTupleHeader t;
    HeapTuple tuple;
    double tbl_val; 
    bool isnull;
    Datum values[3] = {0, 0, 0};  /* Array to store attribute values */
    Datum result;
    bool isnullarr[3] = {false, false, false};  /* Array to track null values */

    /* Extract the tuple and double value from function arguments */
    t = PG_GETARG_HEAPTUPLEHEADER(0);
    tbl_val = PG_GETARG_FLOAT8(1); 

    /* Verify that the return type is a composite type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    /* Extract attributes from the tuple and check for nulls */
    for (int i = 1; i <= 3; i++) {
        values[i - 1] = GetAttributeByNum(t, i, &isnull);
        if (isnull) {
            isnullarr[i - 1] = true;
        }
    }

    /* Check if the double value is not null */
    if (!PG_ARGISNULL(1)) {
        /* Compare double value with existing values and update if necessary */
        if (DatumGetFloat8(values[2]) != 0) {

            if (!isnullarr[0]) { /*we can merge two ifs, but too spaghetti in one line*/
                if (DatumGetFloat8(values[0]) > tbl_val) {
                    values[0] = Float8GetDatum(tbl_val); 
                }
            } 

            if (!isnullarr[1]) {
               /* we checking on NULL here, but that no make a lot sense (0,0,0), 
                if needed performance impruvment we can remove it */
                if (DatumGetFloat8(values[1]) < tbl_val) {
                    values[1] = Float8GetDatum(tbl_val); 
                }
            } 
        } else if (!isnullarr[2]) {
           /* If values[2] (cntr) is zero (first call), update all values and increment the count */
            values[0] = Float8GetDatum(tbl_val);
            values[1] = Float8GetDatum(tbl_val);
            values[2] = Float8GetDatum(DatumGetFloat8(values[2]) + 1); 
        }
    }

    /* Create a new tuple with the updated values */
    tupdesc = BlessTupleDesc(tupdesc);
    tuple = heap_form_tuple(tupdesc, values, isnullarr);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(n_final_mtm);

/* Function for final output formatting (for numeric data type) */
Datum
n_final_mtm(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;
    HeapTupleHeader t;
    bool isnull;
    Datum values[3];
    char *outp;
    const char *work_mem_str, *num_str1, *num_str2;
    int specifier_count = MTM_MAX_ARGS_COUNT;

    /* Verify that the return type is scalar */
    if (get_call_result_type(fcinfo, NULL, & tupdesc) != TYPEFUNC_SCALAR)
      ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
          errmsg("function returning record called in context "
            "that cannot accept type record")));

    /* Extract the state_mtm from function arguments */
    t = PG_GETARG_HEAPTUPLEHEADER(0);

    /* Extract attributes from the tuple */
    for (int i = 1; i <= 3; i++) {
      values[i - 1] = GetAttributeByNum(t, i, &isnull);
    }

    /* Check if the count value is zero and return NULL if true */
    if (DatumGetInt32(DirectFunctionCall2(numeric_cmp, values[2], NumericGetDatum(int64_to_numeric(0)))) == 0) {
      PG_RETURN_NULL();
    }

    /* Get the output format string from configuration or use the default */
    work_mem_str = GetConfigOption("mtm.output_format", true, false) ?: MTM_DEFAULT_OUTPUT_FORMAT;

    /* Check the number of format specifiers in the format string */
    if (specifier_count < count_format_specifiers(work_mem_str)) {
        ereport(ERROR,
                (errmsg("Too many '%%s' format specifiers in format string. Expected no more than 2 (%d args)",
                        specifier_count)));
    }

    /* Normalize numeric values to strings */
    /* we can do somthing like 
        v=PG_GETARG_NUMERIC(0);
        r=numeric_normalize(v);
        f=atof(r)*10; 
        as option for arithmetics
    */
    num_str1 = numeric_normalize(DatumGetNumeric(values[0]));
    num_str2 = numeric_normalize(DatumGetNumeric(values[1]));

    /* Format the output string using the dynamic sprintf function */
    if (num_str1 != NULL && num_str2 != NULL) {
        outp = mtm_dynamic_sprintf(work_mem_str, num_str2, num_str1);
    } else {
        ereport(ERROR,
                (errmsg("Double precision to string conversion failed")));
    }

    PG_RETURN_TEXT_P(cstring_to_text(outp));
}

PG_FUNCTION_INFO_V1(f_final_mtm);

/* Function for final output formatting (for double precision data type) */
Datum
f_final_mtm(PG_FUNCTION_ARGS) {
    TupleDesc tupdesc;
    HeapTupleHeader t ;
    bool isnull;
    Datum values[3];
    char *outp;
    const char *work_mem_str;
    char *num_str1, *num_str2;
    int specifier_count = MTM_MAX_ARGS_COUNT;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning scalar called in context "
                        "that cannot accept type scalar")));

    t = PG_GETARG_HEAPTUPLEHEADER(0);

    for (int i = 1; i <= 3; i++) {
        values[i - 1] = GetAttributeByNum(t, i, &isnull);
    }

    if (DatumGetFloat8(values[2]) == 0) {
        PG_RETURN_NULL();
    }

    work_mem_str = GetConfigOption("mtm.output_format", true, false) ?: MTM_DEFAULT_OUTPUT_FORMAT;

    if (specifier_count < count_format_specifiers(work_mem_str)) {
        ereport(ERROR,
                (errmsg("Too many '%%s' format specifiers in format string. Expected no more than %d args)",
                        specifier_count)));
    }

    /* Convert numeric values to strings */
    num_str1 = psprintf("%lf", DatumGetFloat8(values[0]));
    num_str2 = psprintf("%lf", DatumGetFloat8(values[1]));

    /* Trim trailing zeros from the string representations (like small copy of numeric_normalize) of the numbers */
    float_trim_zeros(num_str1);
    float_trim_zeros(num_str2);

    /* Format the final output string using the specified format */
    if (num_str1 != NULL && num_str2 != NULL) {
        outp = mtm_dynamic_sprintf(work_mem_str, num_str2, num_str1);
    } else {
        ereport(ERROR,
                (errmsg("Double precision to string conversion failed")));
    }

    PG_RETURN_TEXT_P(cstring_to_text(outp));
}

/* Trims trailing zeros from a string representation of a floating-point number */
void float_trim_zeros(char * str) {
  int len,i;
  /* Check if the input string is NULL or empty; nothing to do in this case */
  if (str == NULL || * str == '\0') return;

  len = strlen(str); // Get the length of the string

  i = len - 1;  // Start from the end of the string
  /* Remove trailing zeros */
  while (i >= 0 && str[i] == '0') {
    i--;
  }
  if (i >= 0 && str[i] == '.') {
    i--;
  }

  str[i + 1] = '\0';

  if (i < 0) {
    strcpy(str, "0");
  }
}

/* Creates a formatted string with a variable number of arguments */
char* mtm_dynamic_sprintf(const char *format, ...) {
  int size_needed; // Variable to hold the size of the formatted string
  char *outp;      // Pointer to hold the formatted string
  va_list args;    // Variable argument list for the formatted string

  /* Initialize the argument list */
  va_start(args, format);
  /* Determine the size needed for the formatted string */
  size_needed = vsnprintf(NULL, 0, format, args); 
  /* Clean up the argument list */
  va_end(args);

  /* Allocate memory for the formatted string */
  outp = (char *) palloc(size_needed + 1); // +1 for the null terminator

  /* Reinitialize the argument list */
  va_start(args, format);
  /* Format the string into the allocated memory */
  vsnprintf(outp, size_needed + 1, format, args);
  va_end(args);

  return outp;
}


/* Counts the number of "%s" format specifiers in a format string */
int count_format_specifiers(const char *format) {
    int count = 0;
    const char *temp = format;

    while ((temp = strstr(temp, "%s")) != NULL) {
        count++;
        temp += 2;
    }
    return count;
}
