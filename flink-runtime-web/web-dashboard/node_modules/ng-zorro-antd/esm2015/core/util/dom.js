/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { filterNotEmptyNode } from './check';
/**
 * Silent an event by stopping and preventing it.
 * @param {?} e
 * @return {?}
 */
export function silentEvent(e) {
    e.stopPropagation();
    e.preventDefault();
}
/**
 * @param {?} elem
 * @return {?}
 */
export function getElementOffset(elem) {
    if (!elem.getClientRects().length) {
        return { top: 0, left: 0 };
    }
    /** @type {?} */
    const rect = elem.getBoundingClientRect();
    /** @type {?} */
    const win = (/** @type {?} */ (elem.ownerDocument)).defaultView;
    return {
        top: rect.top + (/** @type {?} */ (win)).pageYOffset,
        left: rect.left + (/** @type {?} */ (win)).pageXOffset
    };
}
/**
 * @param {?} element
 * @return {?}
 */
export function findFirstNotEmptyNode(element) {
    /** @type {?} */
    const children = element.childNodes;
    for (let i = 0; i < children.length; i++) {
        /** @type {?} */
        const node = children.item(i);
        if (filterNotEmptyNode(node)) {
            return node;
        }
    }
    return null;
}
/**
 * @param {?} element
 * @return {?}
 */
export function findLastNotEmptyNode(element) {
    /** @type {?} */
    const children = element.childNodes;
    for (let i = children.length - 1; i >= 0; i--) {
        /** @type {?} */
        const node = children.item(i);
        if (filterNotEmptyNode(node)) {
            return node;
        }
    }
    return null;
}
/**
 * @param {?} parent
 * @return {?}
 */
export function reverseChildNodes(parent) {
    /** @type {?} */
    const children = parent.childNodes;
    /** @type {?} */
    let length = children.length;
    if (length) {
        /** @type {?} */
        const nodes = [];
        children.forEach((/**
         * @param {?} node
         * @param {?} i
         * @return {?}
         */
        (node, i) => nodes[i] = node));
        while (length--) {
            parent.appendChild(nodes[length]);
        }
    }
}
/**
 * @record
 */
export function MouseTouchObserverConfig() { }
if (false) {
    /** @type {?} */
    MouseTouchObserverConfig.prototype.end;
    /** @type {?} */
    MouseTouchObserverConfig.prototype.move;
    /** @type {?} */
    MouseTouchObserverConfig.prototype.pluckKey;
    /** @type {?} */
    MouseTouchObserverConfig.prototype.start;
    /** @type {?|undefined} */
    MouseTouchObserverConfig.prototype.end$;
    /** @type {?|undefined} */
    MouseTouchObserverConfig.prototype.moveResolved$;
    /** @type {?|undefined} */
    MouseTouchObserverConfig.prototype.startPlucked$;
    /**
     * @param {?} e
     * @return {?}
     */
    MouseTouchObserverConfig.prototype.filter = function (e) { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC9kb20udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUVBLE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLFNBQVMsQ0FBQzs7Ozs7O0FBSzdDLE1BQU0sVUFBVSxXQUFXLENBQUMsQ0FBUTtJQUNsQyxDQUFDLENBQUMsZUFBZSxFQUFFLENBQUM7SUFDcEIsQ0FBQyxDQUFDLGNBQWMsRUFBRSxDQUFDO0FBQ3JCLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLGdCQUFnQixDQUFDLElBQWlCO0lBQ2hELElBQUksQ0FBQyxJQUFJLENBQUMsY0FBYyxFQUFFLENBQUMsTUFBTSxFQUFFO1FBQ2pDLE9BQU8sRUFBRSxHQUFHLEVBQUUsQ0FBQyxFQUFFLElBQUksRUFBRSxDQUFDLEVBQUUsQ0FBQztLQUM1Qjs7VUFFSyxJQUFJLEdBQUcsSUFBSSxDQUFDLHFCQUFxQixFQUFFOztVQUNuQyxHQUFHLEdBQUcsbUJBQUEsSUFBSSxDQUFDLGFBQWEsRUFBQyxDQUFDLFdBQVc7SUFDM0MsT0FBTztRQUNMLEdBQUcsRUFBRyxJQUFJLENBQUMsR0FBRyxHQUFHLG1CQUFBLEdBQUcsRUFBQyxDQUFDLFdBQVc7UUFDakMsSUFBSSxFQUFFLElBQUksQ0FBQyxJQUFJLEdBQUcsbUJBQUEsR0FBRyxFQUFDLENBQUMsV0FBVztLQUNuQyxDQUFDO0FBQ0osQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUscUJBQXFCLENBQUMsT0FBb0I7O1VBQ2xELFFBQVEsR0FBRyxPQUFPLENBQUMsVUFBVTtJQUNuQyxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsUUFBUSxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTs7Y0FDbEMsSUFBSSxHQUFHLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO1FBQzdCLElBQUksa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDNUIsT0FBTyxJQUFJLENBQUM7U0FDYjtLQUNGO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxvQkFBb0IsQ0FBQyxPQUFvQjs7VUFDakQsUUFBUSxHQUFHLE9BQU8sQ0FBQyxVQUFVO0lBQ25DLEtBQUssSUFBSSxDQUFDLEdBQUcsUUFBUSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUUsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRTs7Y0FDdkMsSUFBSSxHQUFHLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO1FBQzdCLElBQUksa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDNUIsT0FBTyxJQUFJLENBQUM7U0FDYjtLQUNGO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxpQkFBaUIsQ0FBQyxNQUFtQjs7VUFDN0MsUUFBUSxHQUFHLE1BQU0sQ0FBQyxVQUFVOztRQUM5QixNQUFNLEdBQUcsUUFBUSxDQUFDLE1BQU07SUFDNUIsSUFBSSxNQUFNLEVBQUU7O2NBQ0osS0FBSyxHQUFXLEVBQUU7UUFDeEIsUUFBUSxDQUFDLE9BQU87Ozs7O1FBQUMsQ0FBQyxJQUFJLEVBQUUsQ0FBQyxFQUFFLEVBQUUsQ0FBQyxLQUFLLENBQUUsQ0FBQyxDQUFFLEdBQUcsSUFBSSxFQUFDLENBQUM7UUFDakQsT0FBTyxNQUFNLEVBQUUsRUFBRTtZQUNmLE1BQU0sQ0FBQyxXQUFXLENBQUMsS0FBSyxDQUFFLE1BQU0sQ0FBRSxDQUFDLENBQUM7U0FDckM7S0FDRjtBQUNILENBQUM7Ozs7QUFFRCw4Q0FXQzs7O0lBVkMsdUNBQVk7O0lBQ1osd0NBQWE7O0lBQ2IsNENBQW1COztJQUNuQix5Q0FBYzs7SUFFZCx3Q0FBeUI7O0lBQ3pCLGlEQUFtQzs7SUFDbkMsaURBQW1DOzs7OztJQUVuQyw2REFBMkIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPYnNlcnZhYmxlIH0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7IGZpbHRlck5vdEVtcHR5Tm9kZSB9IGZyb20gJy4vY2hlY2snO1xuXG4vKipcbiAqIFNpbGVudCBhbiBldmVudCBieSBzdG9wcGluZyBhbmQgcHJldmVudGluZyBpdC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHNpbGVudEV2ZW50KGU6IEV2ZW50KTogdm9pZCB7XG4gIGUuc3RvcFByb3BhZ2F0aW9uKCk7XG4gIGUucHJldmVudERlZmF1bHQoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldEVsZW1lbnRPZmZzZXQoZWxlbTogSFRNTEVsZW1lbnQpOiB7IHRvcDogbnVtYmVyLCBsZWZ0OiBudW1iZXIgfSB7XG4gIGlmICghZWxlbS5nZXRDbGllbnRSZWN0cygpLmxlbmd0aCkge1xuICAgIHJldHVybiB7IHRvcDogMCwgbGVmdDogMCB9O1xuICB9XG5cbiAgY29uc3QgcmVjdCA9IGVsZW0uZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCk7XG4gIGNvbnN0IHdpbiA9IGVsZW0ub3duZXJEb2N1bWVudCEuZGVmYXVsdFZpZXc7XG4gIHJldHVybiB7XG4gICAgdG9wIDogcmVjdC50b3AgKyB3aW4hLnBhZ2VZT2Zmc2V0LFxuICAgIGxlZnQ6IHJlY3QubGVmdCArIHdpbiEucGFnZVhPZmZzZXRcbiAgfTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGZpbmRGaXJzdE5vdEVtcHR5Tm9kZShlbGVtZW50OiBIVE1MRWxlbWVudCk6IE5vZGUgfCBudWxsIHtcbiAgY29uc3QgY2hpbGRyZW4gPSBlbGVtZW50LmNoaWxkTm9kZXM7XG4gIGZvciAobGV0IGkgPSAwOyBpIDwgY2hpbGRyZW4ubGVuZ3RoOyBpKyspIHtcbiAgICBjb25zdCBub2RlID0gY2hpbGRyZW4uaXRlbShpKTtcbiAgICBpZiAoZmlsdGVyTm90RW1wdHlOb2RlKG5vZGUpKSB7XG4gICAgICByZXR1cm4gbm9kZTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmaW5kTGFzdE5vdEVtcHR5Tm9kZShlbGVtZW50OiBIVE1MRWxlbWVudCk6IE5vZGUgfCBudWxsIHtcbiAgY29uc3QgY2hpbGRyZW4gPSBlbGVtZW50LmNoaWxkTm9kZXM7XG4gIGZvciAobGV0IGkgPSBjaGlsZHJlbi5sZW5ndGggLSAxOyBpID49IDA7IGktLSkge1xuICAgIGNvbnN0IG5vZGUgPSBjaGlsZHJlbi5pdGVtKGkpO1xuICAgIGlmIChmaWx0ZXJOb3RFbXB0eU5vZGUobm9kZSkpIHtcbiAgICAgIHJldHVybiBub2RlO1xuICAgIH1cbiAgfVxuICByZXR1cm4gbnVsbDtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHJldmVyc2VDaGlsZE5vZGVzKHBhcmVudDogSFRNTEVsZW1lbnQpOiB2b2lkIHtcbiAgY29uc3QgY2hpbGRyZW4gPSBwYXJlbnQuY2hpbGROb2RlcztcbiAgbGV0IGxlbmd0aCA9IGNoaWxkcmVuLmxlbmd0aDtcbiAgaWYgKGxlbmd0aCkge1xuICAgIGNvbnN0IG5vZGVzOiBOb2RlW10gPSBbXTtcbiAgICBjaGlsZHJlbi5mb3JFYWNoKChub2RlLCBpKSA9PiBub2Rlc1sgaSBdID0gbm9kZSk7XG4gICAgd2hpbGUgKGxlbmd0aC0tKSB7XG4gICAgICBwYXJlbnQuYXBwZW5kQ2hpbGQobm9kZXNbIGxlbmd0aCBdKTtcbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGludGVyZmFjZSBNb3VzZVRvdWNoT2JzZXJ2ZXJDb25maWcge1xuICBlbmQ6IHN0cmluZztcbiAgbW92ZTogc3RyaW5nO1xuICBwbHVja0tleTogc3RyaW5nW107XG4gIHN0YXJ0OiBzdHJpbmc7XG5cbiAgZW5kJD86IE9ic2VydmFibGU8RXZlbnQ+O1xuICBtb3ZlUmVzb2x2ZWQkPzogT2JzZXJ2YWJsZTxudW1iZXI+O1xuICBzdGFydFBsdWNrZWQkPzogT2JzZXJ2YWJsZTxudW1iZXI+O1xuXG4gIGZpbHRlcj8oZTogRXZlbnQpOiBib29sZWFuO1xufVxuIl19