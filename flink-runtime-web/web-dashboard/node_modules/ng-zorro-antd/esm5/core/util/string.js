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
    var joined = "" + getRepeatedElement(length, element) + toPad;
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
    var joined = "" + toPad + getRepeatedElement(length, element);
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoic3RyaW5nLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC9zdHJpbmcudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7Ozs7Ozs7QUFHQSxNQUFNLFVBQVUsUUFBUSxDQUFDLEtBQWEsRUFBRSxNQUFjLEVBQUUsT0FBZTtJQUNyRSxJQUFJLEtBQUssQ0FBQyxNQUFNLEdBQUcsTUFBTSxFQUFFO1FBQ3pCLE9BQU8sS0FBSyxDQUFDO0tBQ2Q7O1FBRUssTUFBTSxHQUFHLEtBQUcsa0JBQWtCLENBQUMsTUFBTSxFQUFFLE9BQU8sQ0FBQyxHQUFHLEtBQU87SUFDL0QsT0FBTyxNQUFNLENBQUMsS0FBSyxDQUFDLE1BQU0sQ0FBQyxNQUFNLEdBQUcsTUFBTSxFQUFFLE1BQU0sQ0FBQyxNQUFNLENBQUMsQ0FBQztBQUM3RCxDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLE1BQU0sQ0FBQyxLQUFhLEVBQUUsTUFBYyxFQUFFLE9BQWU7O1FBQzdELE1BQU0sR0FBRyxLQUFHLEtBQUssR0FBRyxrQkFBa0IsQ0FBQyxNQUFNLEVBQUUsT0FBTyxDQUFHO0lBQy9ELE9BQU8sTUFBTSxDQUFDLEtBQUssQ0FBQyxDQUFDLEVBQUUsTUFBTSxDQUFDLENBQUM7QUFDakMsQ0FBQzs7Ozs7O0FBRUQsTUFBTSxVQUFVLGtCQUFrQixDQUFDLE1BQWMsRUFBRSxPQUFlO0lBQ2hFLE9BQU8sS0FBSyxDQUFDLE1BQU0sQ0FBQyxDQUFDLElBQUksQ0FBQyxPQUFPLENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7QUFDOUMsQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8qKlxuICogTXVjaCBsaWtlIGxvZGFzaC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHBhZFN0YXJ0KHRvUGFkOiBzdHJpbmcsIGxlbmd0aDogbnVtYmVyLCBlbGVtZW50OiBzdHJpbmcpOiBzdHJpbmcge1xuICBpZiAodG9QYWQubGVuZ3RoID4gbGVuZ3RoKSB7XG4gICAgcmV0dXJuIHRvUGFkO1xuICB9XG5cbiAgY29uc3Qgam9pbmVkID0gYCR7Z2V0UmVwZWF0ZWRFbGVtZW50KGxlbmd0aCwgZWxlbWVudCl9JHt0b1BhZH1gO1xuICByZXR1cm4gam9pbmVkLnNsaWNlKGpvaW5lZC5sZW5ndGggLSBsZW5ndGgsIGpvaW5lZC5sZW5ndGgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFkRW5kKHRvUGFkOiBzdHJpbmcsIGxlbmd0aDogbnVtYmVyLCBlbGVtZW50OiBzdHJpbmcpOiBzdHJpbmcge1xuICBjb25zdCBqb2luZWQgPSBgJHt0b1BhZH0ke2dldFJlcGVhdGVkRWxlbWVudChsZW5ndGgsIGVsZW1lbnQpfWA7XG4gIHJldHVybiBqb2luZWQuc2xpY2UoMCwgbGVuZ3RoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFJlcGVhdGVkRWxlbWVudChsZW5ndGg6IG51bWJlciwgZWxlbWVudDogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIEFycmF5KGxlbmd0aCkuZmlsbChlbGVtZW50KS5qb2luKCcnKTtcbn1cbiJdfQ==