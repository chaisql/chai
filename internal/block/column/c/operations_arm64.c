#include <stdint.h>
#include <arm_neon.h>

void int64_add_scalar(int64_t *a, int64_t b, int64_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] + b;
    }
}

void int64_sub_scalar(int64_t *a, int64_t b, int64_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] - b;
    }
}

void int64_mul_scalar(int64_t *a, int64_t b, int64_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] * b;
    }
}

void int64_div_scalar(int64_t *a, int64_t b, int64_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] / b;
    }
}

void int64_mod_scalar(int64_t *a, int64_t b, int64_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] % b;
    }
}

void int32_add_scalar(int32_t *a, int32_t b, int32_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] + b;
    }
}

void int32_sub_scalar(int32_t *a, int32_t b, int32_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] - b;
    }
}

void int32_mul_scalar(int32_t *a, int32_t b, int32_t *result, int len) {
    #pragma clang loop vectorize(enable) interleave(enable)
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] * b;
    }
}

void int32_div_scalar(int32_t *a, int32_t b, int32_t *result, int len) {
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] / b;
    }
}

void int32_mod_scalar(int32_t *a, int32_t b, int32_t *result, int len) {
    for (int i = 0; i < len; i++)
    {
        result[i] = a[i] % b;
    }
}
