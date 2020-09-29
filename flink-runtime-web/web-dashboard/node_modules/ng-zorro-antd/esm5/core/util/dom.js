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
    var rect = elem.getBoundingClientRect();
    /** @type {?} */
    var win = (/** @type {?} */ (elem.ownerDocument)).defaultView;
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
    var children = element.childNodes;
    for (var i = 0; i < children.length; i++) {
        /** @type {?} */
        var node = children.item(i);
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
    var children = element.childNodes;
    for (var i = children.length - 1; i >= 0; i--) {
        /** @type {?} */
        var node = children.item(i);
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
    var children = parent.childNodes;
    /** @type {?} */
    var length = children.length;
    if (length) {
        /** @type {?} */
        var nodes_1 = [];
        children.forEach((/**
         * @param {?} node
         * @param {?} i
         * @return {?}
         */
        function (node, i) { return nodes_1[i] = node; }));
        while (length--) {
            parent.appendChild(nodes_1[length]);
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiZG9tLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC9kb20udHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUVBLE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLFNBQVMsQ0FBQzs7Ozs7O0FBSzdDLE1BQU0sVUFBVSxXQUFXLENBQUMsQ0FBUTtJQUNsQyxDQUFDLENBQUMsZUFBZSxFQUFFLENBQUM7SUFDcEIsQ0FBQyxDQUFDLGNBQWMsRUFBRSxDQUFDO0FBQ3JCLENBQUM7Ozs7O0FBRUQsTUFBTSxVQUFVLGdCQUFnQixDQUFDLElBQWlCO0lBQ2hELElBQUksQ0FBQyxJQUFJLENBQUMsY0FBYyxFQUFFLENBQUMsTUFBTSxFQUFFO1FBQ2pDLE9BQU8sRUFBRSxHQUFHLEVBQUUsQ0FBQyxFQUFFLElBQUksRUFBRSxDQUFDLEVBQUUsQ0FBQztLQUM1Qjs7UUFFSyxJQUFJLEdBQUcsSUFBSSxDQUFDLHFCQUFxQixFQUFFOztRQUNuQyxHQUFHLEdBQUcsbUJBQUEsSUFBSSxDQUFDLGFBQWEsRUFBQyxDQUFDLFdBQVc7SUFDM0MsT0FBTztRQUNMLEdBQUcsRUFBRyxJQUFJLENBQUMsR0FBRyxHQUFHLG1CQUFBLEdBQUcsRUFBQyxDQUFDLFdBQVc7UUFDakMsSUFBSSxFQUFFLElBQUksQ0FBQyxJQUFJLEdBQUcsbUJBQUEsR0FBRyxFQUFDLENBQUMsV0FBVztLQUNuQyxDQUFDO0FBQ0osQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUscUJBQXFCLENBQUMsT0FBb0I7O1FBQ2xELFFBQVEsR0FBRyxPQUFPLENBQUMsVUFBVTtJQUNuQyxLQUFLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRSxDQUFDLEdBQUcsUUFBUSxDQUFDLE1BQU0sRUFBRSxDQUFDLEVBQUUsRUFBRTs7WUFDbEMsSUFBSSxHQUFHLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO1FBQzdCLElBQUksa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDNUIsT0FBTyxJQUFJLENBQUM7U0FDYjtLQUNGO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxvQkFBb0IsQ0FBQyxPQUFvQjs7UUFDakQsUUFBUSxHQUFHLE9BQU8sQ0FBQyxVQUFVO0lBQ25DLEtBQUssSUFBSSxDQUFDLEdBQUcsUUFBUSxDQUFDLE1BQU0sR0FBRyxDQUFDLEVBQUUsQ0FBQyxJQUFJLENBQUMsRUFBRSxDQUFDLEVBQUUsRUFBRTs7WUFDdkMsSUFBSSxHQUFHLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDO1FBQzdCLElBQUksa0JBQWtCLENBQUMsSUFBSSxDQUFDLEVBQUU7WUFDNUIsT0FBTyxJQUFJLENBQUM7U0FDYjtLQUNGO0lBQ0QsT0FBTyxJQUFJLENBQUM7QUFDZCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxpQkFBaUIsQ0FBQyxNQUFtQjs7UUFDN0MsUUFBUSxHQUFHLE1BQU0sQ0FBQyxVQUFVOztRQUM5QixNQUFNLEdBQUcsUUFBUSxDQUFDLE1BQU07SUFDNUIsSUFBSSxNQUFNLEVBQUU7O1lBQ0osT0FBSyxHQUFXLEVBQUU7UUFDeEIsUUFBUSxDQUFDLE9BQU87Ozs7O1FBQUMsVUFBQyxJQUFJLEVBQUUsQ0FBQyxJQUFLLE9BQUEsT0FBSyxDQUFFLENBQUMsQ0FBRSxHQUFHLElBQUksRUFBakIsQ0FBaUIsRUFBQyxDQUFDO1FBQ2pELE9BQU8sTUFBTSxFQUFFLEVBQUU7WUFDZixNQUFNLENBQUMsV0FBVyxDQUFDLE9BQUssQ0FBRSxNQUFNLENBQUUsQ0FBQyxDQUFDO1NBQ3JDO0tBQ0Y7QUFDSCxDQUFDOzs7O0FBRUQsOENBV0M7OztJQVZDLHVDQUFZOztJQUNaLHdDQUFhOztJQUNiLDRDQUFtQjs7SUFDbkIseUNBQWM7O0lBRWQsd0NBQXlCOztJQUN6QixpREFBbUM7O0lBQ25DLGlEQUFtQzs7Ozs7SUFFbkMsNkRBQTJCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgT2JzZXJ2YWJsZSB9IGZyb20gJ3J4anMnO1xuXG5pbXBvcnQgeyBmaWx0ZXJOb3RFbXB0eU5vZGUgfSBmcm9tICcuL2NoZWNrJztcblxuLyoqXG4gKiBTaWxlbnQgYW4gZXZlbnQgYnkgc3RvcHBpbmcgYW5kIHByZXZlbnRpbmcgaXQuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBzaWxlbnRFdmVudChlOiBFdmVudCk6IHZvaWQge1xuICBlLnN0b3BQcm9wYWdhdGlvbigpO1xuICBlLnByZXZlbnREZWZhdWx0KCk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRFbGVtZW50T2Zmc2V0KGVsZW06IEhUTUxFbGVtZW50KTogeyB0b3A6IG51bWJlciwgbGVmdDogbnVtYmVyIH0ge1xuICBpZiAoIWVsZW0uZ2V0Q2xpZW50UmVjdHMoKS5sZW5ndGgpIHtcbiAgICByZXR1cm4geyB0b3A6IDAsIGxlZnQ6IDAgfTtcbiAgfVxuXG4gIGNvbnN0IHJlY3QgPSBlbGVtLmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpO1xuICBjb25zdCB3aW4gPSBlbGVtLm93bmVyRG9jdW1lbnQhLmRlZmF1bHRWaWV3O1xuICByZXR1cm4ge1xuICAgIHRvcCA6IHJlY3QudG9wICsgd2luIS5wYWdlWU9mZnNldCxcbiAgICBsZWZ0OiByZWN0LmxlZnQgKyB3aW4hLnBhZ2VYT2Zmc2V0XG4gIH07XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBmaW5kRmlyc3ROb3RFbXB0eU5vZGUoZWxlbWVudDogSFRNTEVsZW1lbnQpOiBOb2RlIHwgbnVsbCB7XG4gIGNvbnN0IGNoaWxkcmVuID0gZWxlbWVudC5jaGlsZE5vZGVzO1xuICBmb3IgKGxldCBpID0gMDsgaSA8IGNoaWxkcmVuLmxlbmd0aDsgaSsrKSB7XG4gICAgY29uc3Qgbm9kZSA9IGNoaWxkcmVuLml0ZW0oaSk7XG4gICAgaWYgKGZpbHRlck5vdEVtcHR5Tm9kZShub2RlKSkge1xuICAgICAgcmV0dXJuIG5vZGU7XG4gICAgfVxuICB9XG4gIHJldHVybiBudWxsO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gZmluZExhc3ROb3RFbXB0eU5vZGUoZWxlbWVudDogSFRNTEVsZW1lbnQpOiBOb2RlIHwgbnVsbCB7XG4gIGNvbnN0IGNoaWxkcmVuID0gZWxlbWVudC5jaGlsZE5vZGVzO1xuICBmb3IgKGxldCBpID0gY2hpbGRyZW4ubGVuZ3RoIC0gMTsgaSA+PSAwOyBpLS0pIHtcbiAgICBjb25zdCBub2RlID0gY2hpbGRyZW4uaXRlbShpKTtcbiAgICBpZiAoZmlsdGVyTm90RW1wdHlOb2RlKG5vZGUpKSB7XG4gICAgICByZXR1cm4gbm9kZTtcbiAgICB9XG4gIH1cbiAgcmV0dXJuIG51bGw7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZXZlcnNlQ2hpbGROb2RlcyhwYXJlbnQ6IEhUTUxFbGVtZW50KTogdm9pZCB7XG4gIGNvbnN0IGNoaWxkcmVuID0gcGFyZW50LmNoaWxkTm9kZXM7XG4gIGxldCBsZW5ndGggPSBjaGlsZHJlbi5sZW5ndGg7XG4gIGlmIChsZW5ndGgpIHtcbiAgICBjb25zdCBub2RlczogTm9kZVtdID0gW107XG4gICAgY2hpbGRyZW4uZm9yRWFjaCgobm9kZSwgaSkgPT4gbm9kZXNbIGkgXSA9IG5vZGUpO1xuICAgIHdoaWxlIChsZW5ndGgtLSkge1xuICAgICAgcGFyZW50LmFwcGVuZENoaWxkKG5vZGVzWyBsZW5ndGggXSk7XG4gICAgfVxuICB9XG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgTW91c2VUb3VjaE9ic2VydmVyQ29uZmlnIHtcbiAgZW5kOiBzdHJpbmc7XG4gIG1vdmU6IHN0cmluZztcbiAgcGx1Y2tLZXk6IHN0cmluZ1tdO1xuICBzdGFydDogc3RyaW5nO1xuXG4gIGVuZCQ/OiBPYnNlcnZhYmxlPEV2ZW50PjtcbiAgbW92ZVJlc29sdmVkJD86IE9ic2VydmFibGU8bnVtYmVyPjtcbiAgc3RhcnRQbHVja2VkJD86IE9ic2VydmFibGU8bnVtYmVyPjtcblxuICBmaWx0ZXI/KGU6IEV2ZW50KTogYm9vbGVhbjtcbn1cbiJdfQ==