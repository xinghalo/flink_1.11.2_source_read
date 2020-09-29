/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @param {?} min
 * @param {?} max
 * @param {?} value
 * @return {?}
 */
export function getPercent(min, max, value) {
    return (value - min) / (max - min) * 100;
}
/**
 * @param {?} num
 * @return {?}
 */
export function getPrecision(num) {
    /** @type {?} */
    const numStr = num.toString();
    /** @type {?} */
    const dotIndex = numStr.indexOf('.');
    return dotIndex >= 0 ? numStr.length - dotIndex - 1 : 0;
}
/**
 * @param {?} num
 * @param {?} min
 * @param {?} max
 * @return {?}
 */
export function ensureNumberInRange(num, min, max) {
    if (isNaN(num) || num < min) {
        return min;
    }
    else if (num > max) {
        return max;
    }
    else {
        return num;
    }
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnVtYmVyLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC9udW1iZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7OztBQUFBLE1BQU0sVUFBVSxVQUFVLENBQUMsR0FBVyxFQUFFLEdBQVcsRUFBRSxLQUFhO0lBQ2hFLE9BQU8sQ0FBQyxLQUFLLEdBQUcsR0FBRyxDQUFDLEdBQUcsQ0FBQyxHQUFHLEdBQUcsR0FBRyxDQUFDLEdBQUcsR0FBRyxDQUFDO0FBQzNDLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLFlBQVksQ0FBQyxHQUFXOztVQUNoQyxNQUFNLEdBQUcsR0FBRyxDQUFDLFFBQVEsRUFBRTs7VUFDdkIsUUFBUSxHQUFHLE1BQU0sQ0FBQyxPQUFPLENBQUMsR0FBRyxDQUFDO0lBQ3BDLE9BQU8sUUFBUSxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUFDLE1BQU0sR0FBRyxRQUFRLEdBQUcsQ0FBQyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7QUFDMUQsQ0FBQzs7Ozs7OztBQUVELE1BQU0sVUFBVSxtQkFBbUIsQ0FBQyxHQUFXLEVBQUUsR0FBVyxFQUFFLEdBQVc7SUFDdkUsSUFBSSxLQUFLLENBQUMsR0FBRyxDQUFDLElBQUksR0FBRyxHQUFHLEdBQUcsRUFBRTtRQUMzQixPQUFPLEdBQUcsQ0FBQztLQUNaO1NBQU0sSUFBSSxHQUFHLEdBQUcsR0FBRyxFQUFFO1FBQ3BCLE9BQU8sR0FBRyxDQUFDO0tBQ1o7U0FBTTtRQUNMLE9BQU8sR0FBRyxDQUFDO0tBQ1o7QUFDSCxDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiZXhwb3J0IGZ1bmN0aW9uIGdldFBlcmNlbnQobWluOiBudW1iZXIsIG1heDogbnVtYmVyLCB2YWx1ZTogbnVtYmVyKTogbnVtYmVyIHtcbiAgcmV0dXJuICh2YWx1ZSAtIG1pbikgLyAobWF4IC0gbWluKSAqIDEwMDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFByZWNpc2lvbihudW06IG51bWJlcik6IG51bWJlciB7XG4gIGNvbnN0IG51bVN0ciA9IG51bS50b1N0cmluZygpO1xuICBjb25zdCBkb3RJbmRleCA9IG51bVN0ci5pbmRleE9mKCcuJyk7XG4gIHJldHVybiBkb3RJbmRleCA+PSAwID8gbnVtU3RyLmxlbmd0aCAtIGRvdEluZGV4IC0gMSA6IDA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBlbnN1cmVOdW1iZXJJblJhbmdlKG51bTogbnVtYmVyLCBtaW46IG51bWJlciwgbWF4OiBudW1iZXIpOiBudW1iZXIge1xuICBpZiAoaXNOYU4obnVtKSB8fCBudW0gPCBtaW4pIHtcbiAgICByZXR1cm4gbWluO1xuICB9IGVsc2UgaWYgKG51bSA+IG1heCkge1xuICAgIHJldHVybiBtYXg7XG4gIH0gZWxzZSB7XG4gICAgcmV0dXJuIG51bTtcbiAgfVxufVxuIl19