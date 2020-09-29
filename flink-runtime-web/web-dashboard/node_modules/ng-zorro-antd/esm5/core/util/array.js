/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * @template T
 * @param {?} value
 * @return {?}
 */
export function toArray(value) {
    /** @type {?} */
    var ret;
    if (value == null) {
        ret = [];
    }
    else if (!Array.isArray(value)) {
        ret = [value];
    }
    else {
        ret = value;
    }
    return ret;
}
/**
 * @template T
 * @param {?} array1
 * @param {?} array2
 * @return {?}
 */
export function arraysEqual(array1, array2) {
    if (!array1 || !array2 || array1.length !== array2.length) {
        return false;
    }
    /** @type {?} */
    var len = array1.length;
    for (var i = 0; i < len; i++) {
        if (array1[i] !== array2[i]) {
            return false;
        }
    }
    return true;
}
/**
 * @template T
 * @param {?} source
 * @return {?}
 */
export function shallowCopyArray(source) {
    return source.slice();
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiYXJyYXkuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS91dGlsL2FycmF5LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7Ozs7OztBQUFBLE1BQU0sVUFBVSxPQUFPLENBQUksS0FBYzs7UUFDbkMsR0FBUTtJQUNaLElBQUksS0FBSyxJQUFJLElBQUksRUFBRTtRQUNqQixHQUFHLEdBQUcsRUFBRSxDQUFDO0tBQ1Y7U0FBTSxJQUFJLENBQUMsS0FBSyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsRUFBRTtRQUNoQyxHQUFHLEdBQUcsQ0FBRSxLQUFLLENBQUUsQ0FBQztLQUNqQjtTQUFNO1FBQ0wsR0FBRyxHQUFHLEtBQUssQ0FBQztLQUNiO0lBQ0QsT0FBTyxHQUFHLENBQUM7QUFDYixDQUFDOzs7Ozs7O0FBRUQsTUFBTSxVQUFVLFdBQVcsQ0FBSSxNQUFXLEVBQUUsTUFBVztJQUNyRCxJQUFJLENBQUMsTUFBTSxJQUFJLENBQUMsTUFBTSxJQUFJLE1BQU0sQ0FBQyxNQUFNLEtBQUssTUFBTSxDQUFDLE1BQU0sRUFBRTtRQUN6RCxPQUFPLEtBQUssQ0FBQztLQUNkOztRQUVLLEdBQUcsR0FBRyxNQUFNLENBQUMsTUFBTTtJQUN6QixLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsR0FBRyxFQUFFLENBQUMsRUFBRSxFQUFFO1FBQzVCLElBQUksTUFBTSxDQUFFLENBQUMsQ0FBRSxLQUFLLE1BQU0sQ0FBRSxDQUFDLENBQUUsRUFBRTtZQUMvQixPQUFPLEtBQUssQ0FBQztTQUNkO0tBQ0Y7SUFDRCxPQUFPLElBQUksQ0FBQztBQUNkLENBQUM7Ozs7OztBQUVELE1BQU0sVUFBVSxnQkFBZ0IsQ0FBSSxNQUFXO0lBQzdDLE9BQU8sTUFBTSxDQUFDLEtBQUssRUFBRSxDQUFDO0FBQ3hCLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyJleHBvcnQgZnVuY3Rpb24gdG9BcnJheTxUPih2YWx1ZTogVCB8IFRbXSk6IFRbXSB7XG4gIGxldCByZXQ6IFRbXTtcbiAgaWYgKHZhbHVlID09IG51bGwpIHtcbiAgICByZXQgPSBbXTtcbiAgfSBlbHNlIGlmICghQXJyYXkuaXNBcnJheSh2YWx1ZSkpIHtcbiAgICByZXQgPSBbIHZhbHVlIF07XG4gIH0gZWxzZSB7XG4gICAgcmV0ID0gdmFsdWU7XG4gIH1cbiAgcmV0dXJuIHJldDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGFycmF5c0VxdWFsPFQ+KGFycmF5MTogVFtdLCBhcnJheTI6IFRbXSk6IGJvb2xlYW4ge1xuICBpZiAoIWFycmF5MSB8fCAhYXJyYXkyIHx8IGFycmF5MS5sZW5ndGggIT09IGFycmF5Mi5sZW5ndGgpIHtcbiAgICByZXR1cm4gZmFsc2U7XG4gIH1cblxuICBjb25zdCBsZW4gPSBhcnJheTEubGVuZ3RoO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgaWYgKGFycmF5MVsgaSBdICE9PSBhcnJheTJbIGkgXSkge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gdHJ1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNoYWxsb3dDb3B5QXJyYXk8VD4oc291cmNlOiBUW10pOiBUW10ge1xuICByZXR1cm4gc291cmNlLnNsaWNlKCk7XG59XG4iXX0=