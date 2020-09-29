/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { TemplateRef, Type } from '@angular/core';
// tslint:disable-next-line:no-any
/**
 * @param {?} value
 * @return {?}
 */
export function isNotNil(value) {
    return (typeof (value) !== 'undefined') && value !== null;
}
// tslint:disable-next-line:no-any
/**
 * @param {?} value
 * @return {?}
 */
export function isNil(value) {
    return (typeof (value) === 'undefined') || value === null;
}
/**
 * Examine if two objects are shallowly equaled.
 * @param {?=} objA
 * @param {?=} objB
 * @return {?}
 */
export function shallowEqual(objA, objB) {
    if (objA === objB) {
        return true;
    }
    if (typeof objA !== 'object' || !objA || typeof objB !== 'object' || !objB) {
        return false;
    }
    /** @type {?} */
    const keysA = Object.keys(objA);
    /** @type {?} */
    const keysB = Object.keys(objB);
    if (keysA.length !== keysB.length) {
        return false;
    }
    /** @type {?} */
    const bHasOwnProperty = Object.prototype.hasOwnProperty.bind(objB);
    // tslint:disable-next-line:prefer-for-of
    for (let idx = 0; idx < keysA.length; idx++) {
        /** @type {?} */
        const key = keysA[idx];
        if (!bHasOwnProperty(key)) {
            return false;
        }
        if (objA[key] !== objB[key]) {
            return false;
        }
    }
    return true;
}
/**
 * @param {?} value
 * @return {?}
 */
export function isInteger(value) {
    return typeof value === 'number' &&
        isFinite(value) &&
        Math.floor(value) === value;
}
/**
 * @param {?} element
 * @return {?}
 */
export function isEmpty(element) {
    /** @type {?} */
    const nodes = element.childNodes;
    for (let i = 0; i < nodes.length; i++) {
        if (filterNotEmptyNode(nodes.item(i))) {
            return false;
        }
    }
    return true;
}
/**
 * @param {?} node
 * @return {?}
 */
export function filterNotEmptyNode(node) {
    if (node) {
        if ((node.nodeType === 1) && (((/** @type {?} */ (node))).outerHTML.toString().trim().length !== 0)) {
            // ELEMENT_NODE
            return node;
        }
        else if ((node.nodeType === 3) && ((/** @type {?} */ (node.textContent)).toString().trim().length !== 0)) {
            // TEXT_NODE
            return node;
        }
        return null;
    }
    return null;
}
/**
 * @param {?} value
 * @return {?}
 */
export function isNonEmptyString(value) {
    return typeof value === 'string' && value !== '';
}
/**
 * @param {?} value
 * @return {?}
 */
export function isTemplateRef(value) {
    return value instanceof TemplateRef;
}
/**
 * @param {?} value
 * @return {?}
 */
export function isComponent(value) {
    return value instanceof Type;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiY2hlY2suanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS91dGlsL2NoZWNrLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsV0FBVyxFQUFFLElBQUksRUFBRSxNQUFNLGVBQWUsQ0FBQzs7Ozs7O0FBSWxELE1BQU0sVUFBVSxRQUFRLENBQUMsS0FBVTtJQUNqQyxPQUFPLENBQUMsT0FBTSxDQUFDLEtBQUssQ0FBQyxLQUFLLFdBQVcsQ0FBQyxJQUFJLEtBQUssS0FBSyxJQUFJLENBQUM7QUFDM0QsQ0FBQzs7Ozs7O0FBR0QsTUFBTSxVQUFVLEtBQUssQ0FBQyxLQUFVO0lBQzlCLE9BQU8sQ0FBQyxPQUFNLENBQUMsS0FBSyxDQUFDLEtBQUssV0FBVyxDQUFDLElBQUksS0FBSyxLQUFLLElBQUksQ0FBQztBQUMzRCxDQUFDOzs7Ozs7O0FBS0QsTUFBTSxVQUFVLFlBQVksQ0FBQyxJQUFzQixFQUFFLElBQXNCO0lBQ3pFLElBQUksSUFBSSxLQUFLLElBQUksRUFBRTtRQUNqQixPQUFPLElBQUksQ0FBQztLQUNiO0lBRUQsSUFBSSxPQUFPLElBQUksS0FBSyxRQUFRLElBQUksQ0FBQyxJQUFJLElBQUksT0FBTyxJQUFJLEtBQUssUUFBUSxJQUFJLENBQUMsSUFBSSxFQUFFO1FBQzFFLE9BQU8sS0FBSyxDQUFDO0tBQ2Q7O1VBRUssS0FBSyxHQUFHLE1BQU0sQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDOztVQUN6QixLQUFLLEdBQUcsTUFBTSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUM7SUFFL0IsSUFBSSxLQUFLLENBQUMsTUFBTSxLQUFLLEtBQUssQ0FBQyxNQUFNLEVBQUU7UUFDakMsT0FBTyxLQUFLLENBQUM7S0FDZDs7VUFFSyxlQUFlLEdBQUcsTUFBTSxDQUFDLFNBQVMsQ0FBQyxjQUFjLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBQztJQUVsRSx5Q0FBeUM7SUFDekMsS0FBSyxJQUFJLEdBQUcsR0FBRyxDQUFDLEVBQUUsR0FBRyxHQUFHLEtBQUssQ0FBQyxNQUFNLEVBQUUsR0FBRyxFQUFFLEVBQUU7O2NBQ3JDLEdBQUcsR0FBRyxLQUFLLENBQUUsR0FBRyxDQUFFO1FBQ3hCLElBQUksQ0FBQyxlQUFlLENBQUMsR0FBRyxDQUFDLEVBQUU7WUFDekIsT0FBTyxLQUFLLENBQUM7U0FDZDtRQUNELElBQUksSUFBSSxDQUFFLEdBQUcsQ0FBRSxLQUFLLElBQUksQ0FBRSxHQUFHLENBQUUsRUFBRTtZQUMvQixPQUFPLEtBQUssQ0FBQztTQUNkO0tBQ0Y7SUFFRCxPQUFPLElBQUksQ0FBQztBQUNkLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLFNBQVMsQ0FBQyxLQUFzQjtJQUM5QyxPQUFPLE9BQU8sS0FBSyxLQUFLLFFBQVE7UUFDOUIsUUFBUSxDQUFDLEtBQUssQ0FBQztRQUNmLElBQUksQ0FBQyxLQUFLLENBQUMsS0FBSyxDQUFDLEtBQUssS0FBSyxDQUFDO0FBQ2hDLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLE9BQU8sQ0FBQyxPQUFvQjs7VUFDcEMsS0FBSyxHQUFHLE9BQU8sQ0FBQyxVQUFVO0lBQ2hDLEtBQUssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxLQUFLLENBQUMsTUFBTSxFQUFFLENBQUMsRUFBRSxFQUFFO1FBQ3JDLElBQUksa0JBQWtCLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxFQUFFO1lBQ3JDLE9BQU8sS0FBSyxDQUFDO1NBQ2Q7S0FDRjtJQUNELE9BQU8sSUFBSSxDQUFDO0FBQ2QsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsa0JBQWtCLENBQUMsSUFBVTtJQUMzQyxJQUFJLElBQUksRUFBRTtRQUNSLElBQUksQ0FBQyxJQUFJLENBQUMsUUFBUSxLQUFLLENBQUMsQ0FBQyxJQUFJLENBQUMsQ0FBQyxtQkFBQSxJQUFJLEVBQWUsQ0FBQyxDQUFDLFNBQVMsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxJQUFJLEVBQUUsQ0FBQyxNQUFNLEtBQUssQ0FBQyxDQUFDLEVBQUU7WUFDN0YsZUFBZTtZQUNmLE9BQU8sSUFBSSxDQUFDO1NBQ2I7YUFBTSxJQUFJLENBQUMsSUFBSSxDQUFDLFFBQVEsS0FBSyxDQUFDLENBQUMsSUFBSSxDQUFDLG1CQUFBLElBQUksQ0FBQyxXQUFXLEVBQUMsQ0FBQyxRQUFRLEVBQUUsQ0FBQyxJQUFJLEVBQUUsQ0FBQyxNQUFNLEtBQUssQ0FBQyxDQUFDLEVBQUU7WUFDdEYsWUFBWTtZQUNaLE9BQU8sSUFBSSxDQUFDO1NBQ2I7UUFDRCxPQUFPLElBQUksQ0FBQztLQUNiO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxnQkFBZ0IsQ0FBQyxLQUFVO0lBQ3pDLE9BQU8sT0FBTyxLQUFLLEtBQUssUUFBUSxJQUFJLEtBQUssS0FBSyxFQUFFLENBQUM7QUFDbkQsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsYUFBYSxDQUFDLEtBQVU7SUFDdEMsT0FBTyxLQUFLLFlBQVksV0FBVyxDQUFDO0FBQ3RDLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLFdBQVcsQ0FBQyxLQUFVO0lBQ3BDLE9BQU8sS0FBSyxZQUFZLElBQUksQ0FBQztBQUMvQixDQUFDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgVGVtcGxhdGVSZWYsIFR5cGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IEluZGV4YWJsZU9iamVjdCB9IGZyb20gJy4uL3R5cGVzL2luZGV4YWJsZSc7XG5cbi8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbmV4cG9ydCBmdW5jdGlvbiBpc05vdE5pbCh2YWx1ZTogYW55KTogYm9vbGVhbiB7XG4gIHJldHVybiAodHlwZW9mKHZhbHVlKSAhPT0gJ3VuZGVmaW5lZCcpICYmIHZhbHVlICE9PSBudWxsO1xufVxuXG4vLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG5leHBvcnQgZnVuY3Rpb24gaXNOaWwodmFsdWU6IGFueSk6IHZhbHVlIGlzIG51bGwgfCB1bmRlZmluZWQge1xuICByZXR1cm4gKHR5cGVvZih2YWx1ZSkgPT09ICd1bmRlZmluZWQnKSB8fCB2YWx1ZSA9PT0gbnVsbDtcbn1cblxuLyoqXG4gKiBFeGFtaW5lIGlmIHR3byBvYmplY3RzIGFyZSBzaGFsbG93bHkgZXF1YWxlZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHNoYWxsb3dFcXVhbChvYmpBPzogSW5kZXhhYmxlT2JqZWN0LCBvYmpCPzogSW5kZXhhYmxlT2JqZWN0KTogYm9vbGVhbiB7XG4gIGlmIChvYmpBID09PSBvYmpCKSB7XG4gICAgcmV0dXJuIHRydWU7XG4gIH1cblxuICBpZiAodHlwZW9mIG9iakEgIT09ICdvYmplY3QnIHx8ICFvYmpBIHx8IHR5cGVvZiBvYmpCICE9PSAnb2JqZWN0JyB8fCAhb2JqQikge1xuICAgIHJldHVybiBmYWxzZTtcbiAgfVxuXG4gIGNvbnN0IGtleXNBID0gT2JqZWN0LmtleXMob2JqQSk7XG4gIGNvbnN0IGtleXNCID0gT2JqZWN0LmtleXMob2JqQik7XG5cbiAgaWYgKGtleXNBLmxlbmd0aCAhPT0ga2V5c0IubGVuZ3RoKSB7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9XG5cbiAgY29uc3QgYkhhc093blByb3BlcnR5ID0gT2JqZWN0LnByb3RvdHlwZS5oYXNPd25Qcm9wZXJ0eS5iaW5kKG9iakIpO1xuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpwcmVmZXItZm9yLW9mXG4gIGZvciAobGV0IGlkeCA9IDA7IGlkeCA8IGtleXNBLmxlbmd0aDsgaWR4KyspIHtcbiAgICBjb25zdCBrZXkgPSBrZXlzQVsgaWR4IF07XG4gICAgaWYgKCFiSGFzT3duUHJvcGVydHkoa2V5KSkge1xuICAgICAgcmV0dXJuIGZhbHNlO1xuICAgIH1cbiAgICBpZiAob2JqQVsga2V5IF0gIT09IG9iakJbIGtleSBdKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIHRydWU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0ludGVnZXIodmFsdWU6IHN0cmluZyB8IG51bWJlcik6IGJvb2xlYW4ge1xuICByZXR1cm4gdHlwZW9mIHZhbHVlID09PSAnbnVtYmVyJyAmJlxuICAgIGlzRmluaXRlKHZhbHVlKSAmJlxuICAgIE1hdGguZmxvb3IodmFsdWUpID09PSB2YWx1ZTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzRW1wdHkoZWxlbWVudDogSFRNTEVsZW1lbnQpOiBib29sZWFuIHtcbiAgY29uc3Qgbm9kZXMgPSBlbGVtZW50LmNoaWxkTm9kZXM7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgbm9kZXMubGVuZ3RoOyBpKyspIHtcbiAgICBpZiAoZmlsdGVyTm90RW1wdHlOb2RlKG5vZGVzLml0ZW0oaSkpKSB7XG4gICAgICByZXR1cm4gZmFsc2U7XG4gICAgfVxuICB9XG4gIHJldHVybiB0cnVlO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZmlsdGVyTm90RW1wdHlOb2RlKG5vZGU6IE5vZGUpOiBOb2RlIHwgbnVsbCB7XG4gIGlmIChub2RlKSB7XG4gICAgaWYgKChub2RlLm5vZGVUeXBlID09PSAxKSAmJiAoKG5vZGUgYXMgSFRNTEVsZW1lbnQpLm91dGVySFRNTC50b1N0cmluZygpLnRyaW0oKS5sZW5ndGggIT09IDApKSB7XG4gICAgICAvLyBFTEVNRU5UX05PREVcbiAgICAgIHJldHVybiBub2RlO1xuICAgIH0gZWxzZSBpZiAoKG5vZGUubm9kZVR5cGUgPT09IDMpICYmIChub2RlLnRleHRDb250ZW50IS50b1N0cmluZygpLnRyaW0oKS5sZW5ndGggIT09IDApKSB7XG4gICAgICAvLyBURVhUX05PREVcbiAgICAgIHJldHVybiBub2RlO1xuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzTm9uRW1wdHlTdHJpbmcodmFsdWU6IGFueSk6IGJvb2xlYW4geyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuICByZXR1cm4gdHlwZW9mIHZhbHVlID09PSAnc3RyaW5nJyAmJiB2YWx1ZSAhPT0gJyc7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1RlbXBsYXRlUmVmKHZhbHVlOiBhbnkpOiBib29sZWFuIHsgLy8gdHNsaW50OmRpc2FibGUtbGluZTpuby1hbnlcbiAgcmV0dXJuIHZhbHVlIGluc3RhbmNlb2YgVGVtcGxhdGVSZWY7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0NvbXBvbmVudCh2YWx1ZTogYW55KTogYm9vbGVhbiB7IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gIHJldHVybiB2YWx1ZSBpbnN0YW5jZW9mIFR5cGU7XG59XG4iXX0=