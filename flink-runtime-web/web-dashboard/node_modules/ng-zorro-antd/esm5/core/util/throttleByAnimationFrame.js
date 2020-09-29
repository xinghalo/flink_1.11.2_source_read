/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
// tslint:disable:no-any typedef no-invalid-this
import { cancelRequestAnimationFrame, reqAnimFrame } from '../polyfill/request-animation';
/**
 * @param {?} fn
 * @return {?}
 */
export default function throttleByAnimationFrame(fn) {
    /** @type {?} */
    var requestId;
    /** @type {?} */
    var later = (/**
     * @param {?} args
     * @return {?}
     */
    function (args) { return (/**
     * @return {?}
     */
    function () {
        requestId = null;
        fn.apply(void 0, tslib_1.__spread(args));
    }); });
    /** @type {?} */
    var throttled = (/**
     * @param {...?} args
     * @return {?}
     */
    function () {
        var args = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            args[_i] = arguments[_i];
        }
        if (requestId == null) {
            requestId = reqAnimFrame(later(args));
        }
    });
    // tslint:disable-next-line:no-non-null-assertion
    ((/** @type {?} */ (throttled))).cancel = (/**
     * @return {?}
     */
    function () { return cancelRequestAnimationFrame((/** @type {?} */ (requestId))); });
    return throttled;
}
/**
 * @return {?}
 */
export function throttleByAnimationFrameDecorator() {
    return (/**
     * @param {?} target
     * @param {?} key
     * @param {?} descriptor
     * @return {?}
     */
    function (target, key, descriptor) {
        /** @type {?} */
        var fn = descriptor.value;
        /** @type {?} */
        var definingProperty = false;
        return {
            configurable: true,
            get: /**
             * @return {?}
             */
            function () {
                if (definingProperty || this === target.prototype || this.hasOwnProperty(key)) {
                    return fn;
                }
                /** @type {?} */
                var boundFn = throttleByAnimationFrame(fn.bind(this));
                definingProperty = true;
                Object.defineProperty(this, key, {
                    value: boundFn,
                    configurable: true,
                    writable: true
                });
                definingProperty = false;
                return boundFn;
            }
        };
    });
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGhyb3R0bGVCeUFuaW1hdGlvbkZyYW1lLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC90aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7O0FBQ0EsT0FBTyxFQUFFLDJCQUEyQixFQUFFLFlBQVksRUFBRSxNQUFNLCtCQUErQixDQUFDOzs7OztBQUUxRixNQUFNLENBQUMsT0FBTyxVQUFVLHdCQUF3QixDQUFDLEVBQU87O1FBQ2xELFNBQXdCOztRQUV0QixLQUFLOzs7O0lBQUcsVUFBQyxJQUFXOzs7SUFBSztRQUM3QixTQUFTLEdBQUcsSUFBSSxDQUFDO1FBQ2pCLEVBQUUsZ0NBQUksSUFBSSxHQUFFO0lBQ2QsQ0FBQyxJQUFBLENBQUE7O1FBRUssU0FBUzs7OztJQUFHO1FBQUMsY0FBYzthQUFkLFVBQWMsRUFBZCxxQkFBYyxFQUFkLElBQWM7WUFBZCx5QkFBYzs7UUFDL0IsSUFBSSxTQUFTLElBQUksSUFBSSxFQUFFO1lBQ3JCLFNBQVMsR0FBRyxZQUFZLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUM7U0FDdkM7SUFDSCxDQUFDLENBQUE7SUFFRCxpREFBaUQ7SUFDakQsQ0FBQyxtQkFBQSxTQUFTLEVBQU8sQ0FBQyxDQUFDLE1BQU07OztJQUFHLGNBQU0sT0FBQSwyQkFBMkIsQ0FBQyxtQkFBQSxTQUFTLEVBQUMsQ0FBQyxFQUF2QyxDQUF1QyxDQUFBLENBQUM7SUFFMUUsT0FBTyxTQUFTLENBQUM7QUFDbkIsQ0FBQzs7OztBQUVELE1BQU0sVUFBVSxpQ0FBaUM7SUFDL0M7Ozs7OztJQUFPLFVBQVUsTUFBVyxFQUFFLEdBQVcsRUFBRSxVQUFlOztZQUNsRCxFQUFFLEdBQUcsVUFBVSxDQUFDLEtBQUs7O1lBQ3ZCLGdCQUFnQixHQUFHLEtBQUs7UUFDNUIsT0FBTztZQUNMLFlBQVksRUFBRSxJQUFJO1lBQ2xCLEdBQUc7Ozs7Z0JBQ0QsSUFBSSxnQkFBZ0IsSUFBSSxJQUFJLEtBQUssTUFBTSxDQUFDLFNBQVMsSUFBSSxJQUFJLENBQUMsY0FBYyxDQUFDLEdBQUcsQ0FBQyxFQUFFO29CQUM3RSxPQUFPLEVBQUUsQ0FBQztpQkFDWDs7b0JBRUssT0FBTyxHQUFHLHdCQUF3QixDQUFDLEVBQUUsQ0FBQyxJQUFJLENBQUMsSUFBSSxDQUFDLENBQUM7Z0JBQ3ZELGdCQUFnQixHQUFHLElBQUksQ0FBQztnQkFDeEIsTUFBTSxDQUFDLGNBQWMsQ0FBQyxJQUFJLEVBQUUsR0FBRyxFQUFFO29CQUMvQixLQUFLLEVBQVMsT0FBTztvQkFDckIsWUFBWSxFQUFFLElBQUk7b0JBQ2xCLFFBQVEsRUFBTSxJQUFJO2lCQUNuQixDQUFDLENBQUM7Z0JBQ0gsZ0JBQWdCLEdBQUcsS0FBSyxDQUFDO2dCQUN6QixPQUFPLE9BQU8sQ0FBQztZQUNqQixDQUFDO1NBQ0YsQ0FBQztJQUNKLENBQUMsRUFBQztBQUNKLENBQUMiLCJzb3VyY2VzQ29udGVudCI6WyIvLyB0c2xpbnQ6ZGlzYWJsZTpuby1hbnkgdHlwZWRlZiBuby1pbnZhbGlkLXRoaXNcbmltcG9ydCB7IGNhbmNlbFJlcXVlc3RBbmltYXRpb25GcmFtZSwgcmVxQW5pbUZyYW1lIH0gZnJvbSAnLi4vcG9seWZpbGwvcmVxdWVzdC1hbmltYXRpb24nO1xuXG5leHBvcnQgZGVmYXVsdCBmdW5jdGlvbiB0aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWUoZm46IGFueSkge1xuICBsZXQgcmVxdWVzdElkOiBudW1iZXIgfCBudWxsO1xuXG4gIGNvbnN0IGxhdGVyID0gKGFyZ3M6IGFueVtdKSA9PiAoKSA9PiB7XG4gICAgcmVxdWVzdElkID0gbnVsbDtcbiAgICBmbiguLi5hcmdzKTtcbiAgfTtcblxuICBjb25zdCB0aHJvdHRsZWQgPSAoLi4uYXJnczogYW55W10pID0+IHtcbiAgICBpZiAocmVxdWVzdElkID09IG51bGwpIHtcbiAgICAgIHJlcXVlc3RJZCA9IHJlcUFuaW1GcmFtZShsYXRlcihhcmdzKSk7XG4gICAgfVxuICB9O1xuXG4gIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1ub24tbnVsbC1hc3NlcnRpb25cbiAgKHRocm90dGxlZCBhcyBhbnkpLmNhbmNlbCA9ICgpID0+IGNhbmNlbFJlcXVlc3RBbmltYXRpb25GcmFtZShyZXF1ZXN0SWQhKTtcblxuICByZXR1cm4gdGhyb3R0bGVkO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gdGhyb3R0bGVCeUFuaW1hdGlvbkZyYW1lRGVjb3JhdG9yKCkge1xuICByZXR1cm4gZnVuY3Rpb24gKHRhcmdldDogYW55LCBrZXk6IHN0cmluZywgZGVzY3JpcHRvcjogYW55KSB7XG4gICAgY29uc3QgZm4gPSBkZXNjcmlwdG9yLnZhbHVlO1xuICAgIGxldCBkZWZpbmluZ1Byb3BlcnR5ID0gZmFsc2U7XG4gICAgcmV0dXJuIHtcbiAgICAgIGNvbmZpZ3VyYWJsZTogdHJ1ZSxcbiAgICAgIGdldCgpIHtcbiAgICAgICAgaWYgKGRlZmluaW5nUHJvcGVydHkgfHwgdGhpcyA9PT0gdGFyZ2V0LnByb3RvdHlwZSB8fCB0aGlzLmhhc093blByb3BlcnR5KGtleSkpIHtcbiAgICAgICAgICByZXR1cm4gZm47XG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCBib3VuZEZuID0gdGhyb3R0bGVCeUFuaW1hdGlvbkZyYW1lKGZuLmJpbmQodGhpcykpO1xuICAgICAgICBkZWZpbmluZ1Byb3BlcnR5ID0gdHJ1ZTtcbiAgICAgICAgT2JqZWN0LmRlZmluZVByb3BlcnR5KHRoaXMsIGtleSwge1xuICAgICAgICAgIHZhbHVlICAgICAgIDogYm91bmRGbixcbiAgICAgICAgICBjb25maWd1cmFibGU6IHRydWUsXG4gICAgICAgICAgd3JpdGFibGUgICAgOiB0cnVlXG4gICAgICAgIH0pO1xuICAgICAgICBkZWZpbmluZ1Byb3BlcnR5ID0gZmFsc2U7XG4gICAgICAgIHJldHVybiBib3VuZEZuO1xuICAgICAgfVxuICAgIH07XG4gIH07XG59XG4iXX0=