/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * Much like lodash.
 * @param {?} toPad
 * @param {?} length
 * @param {?} element
 * @return {?}
 */
export function padStart(toPad, length, element) {
    if (toPad.length > length) {
        return toPad;
    }
    /** @type {?} */
    const joined = `${getRepeatedElement(length, element)}${toPad}`;
    return joined.slice(joined.length - length, joined.length);
}
/**
 * @param {?} toPad
 * @param {?} length
 * @param {?} element
 * @return {?}
 */
export function padEnd(toPad, length, element) {
    /** @type {?} */
    const joined = `${toPad}${getRepeatedElement(length, element)}`;
    return joined.slice(0, length);
}
/**
 * @param {?} length
 * @param {?} element
 * @return {?}
 */
export function getRepeatedElement(length, element) {
    return Array(length).fill(element).join('');
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RyaW5nLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC9zdHJpbmcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFHQSxNQUFNLFVBQVUsUUFBUSxDQUFDLEtBQWEsRUFBRSxNQUFjLEVBQUUsT0FBZTtJQUNyRSxJQUFJLEtBQUssQ0FBQyxNQUFNLEdBQUcsTUFBTSxFQUFFO1FBQ3pCLE9BQU8sS0FBSyxDQUFDO0tBQ2Q7O1VBRUssTUFBTSxHQUFHLEdBQUcsa0JBQWtCLENBQUMsTUFBTSxFQUFFLE9BQU8sQ0FBQyxHQUFHLEtBQUssRUFBRTtJQUMvRCxPQUFPLE1BQU0sQ0FBQyxLQUFLLENBQUMsTUFBTSxDQUFDLE1BQU0sR0FBRyxNQUFNLEVBQUUsTUFBTSxDQUFDLE1BQU0sQ0FBQyxDQUFDO0FBQzdELENBQUM7Ozs7Ozs7QUFFRCxNQUFNLFVBQVUsTUFBTSxDQUFDLEtBQWEsRUFBRSxNQUFjLEVBQUUsT0FBZTs7VUFDN0QsTUFBTSxHQUFHLEdBQUcsS0FBSyxHQUFHLGtCQUFrQixDQUFDLE1BQU0sRUFBRSxPQUFPLENBQUMsRUFBRTtJQUMvRCxPQUFPLE1BQU0sQ0FBQyxLQUFLLENBQUMsQ0FBQyxFQUFFLE1BQU0sQ0FBQyxDQUFDO0FBQ2pDLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxrQkFBa0IsQ0FBQyxNQUFjLEVBQUUsT0FBZTtJQUNoRSxPQUFPLEtBQUssQ0FBQyxNQUFNLENBQUMsQ0FBQyxJQUFJLENBQUMsT0FBTyxDQUFDLENBQUMsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0FBQzlDLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvKipcbiAqIE11Y2ggbGlrZSBsb2Rhc2guXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwYWRTdGFydCh0b1BhZDogc3RyaW5nLCBsZW5ndGg6IG51bWJlciwgZWxlbWVudDogc3RyaW5nKTogc3RyaW5nIHtcbiAgaWYgKHRvUGFkLmxlbmd0aCA+IGxlbmd0aCkge1xuICAgIHJldHVybiB0b1BhZDtcbiAgfVxuXG4gIGNvbnN0IGpvaW5lZCA9IGAke2dldFJlcGVhdGVkRWxlbWVudChsZW5ndGgsIGVsZW1lbnQpfSR7dG9QYWR9YDtcbiAgcmV0dXJuIGpvaW5lZC5zbGljZShqb2luZWQubGVuZ3RoIC0gbGVuZ3RoLCBqb2luZWQubGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhZEVuZCh0b1BhZDogc3RyaW5nLCBsZW5ndGg6IG51bWJlciwgZWxlbWVudDogc3RyaW5nKTogc3RyaW5nIHtcbiAgY29uc3Qgam9pbmVkID0gYCR7dG9QYWR9JHtnZXRSZXBlYXRlZEVsZW1lbnQobGVuZ3RoLCBlbGVtZW50KX1gO1xuICByZXR1cm4gam9pbmVkLnNsaWNlKDAsIGxlbmd0aCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRSZXBlYXRlZEVsZW1lbnQobGVuZ3RoOiBudW1iZXIsIGVsZW1lbnQ6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBBcnJheShsZW5ndGgpLmZpbGwoZWxlbWVudCkuam9pbignJyk7XG59XG4iXX0=