/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
// tslint:disable:no-any typedef no-invalid-this
import { cancelRequestAnimationFrame, reqAnimFrame } from '../polyfill/request-animation';
/**
 * @param {?} fn
 * @return {?}
 */
export default function throttleByAnimationFrame(fn) {
    /** @type {?} */
    let requestId;
    /** @type {?} */
    const later = (/**
     * @param {?} args
     * @return {?}
     */
    (args) => (/**
     * @return {?}
     */
    () => {
        requestId = null;
        fn(...args);
    }));
    /** @type {?} */
    const throttled = (/**
     * @param {...?} args
     * @return {?}
     */
    (...args) => {
        if (requestId == null) {
            requestId = reqAnimFrame(later(args));
        }
    });
    // tslint:disable-next-line:no-non-null-assertion
    ((/** @type {?} */ (throttled))).cancel = (/**
     * @return {?}
     */
    () => cancelRequestAnimationFrame((/** @type {?} */ (requestId))));
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
        const fn = descriptor.value;
        /** @type {?} */
        let definingProperty = false;
        return {
            configurable: true,
            /**
             * @return {?}
             */
            get() {
                if (definingProperty || this === target.prototype || this.hasOwnProperty(key)) {
                    return fn;
                }
                /** @type {?} */
                const boundFn = throttleByAnimationFrame(fn.bind(this));
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGhyb3R0bGVCeUFuaW1hdGlvbkZyYW1lLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNvcmUvdXRpbC90aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWUudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFDQSxPQUFPLEVBQUUsMkJBQTJCLEVBQUUsWUFBWSxFQUFFLE1BQU0sK0JBQStCLENBQUM7Ozs7O0FBRTFGLE1BQU0sQ0FBQyxPQUFPLFVBQVUsd0JBQXdCLENBQUMsRUFBTzs7UUFDbEQsU0FBd0I7O1VBRXRCLEtBQUs7Ozs7SUFBRyxDQUFDLElBQVcsRUFBRSxFQUFFOzs7SUFBQyxHQUFHLEVBQUU7UUFDbEMsU0FBUyxHQUFHLElBQUksQ0FBQztRQUNqQixFQUFFLENBQUMsR0FBRyxJQUFJLENBQUMsQ0FBQztJQUNkLENBQUMsQ0FBQSxDQUFBOztVQUVLLFNBQVM7Ozs7SUFBRyxDQUFDLEdBQUcsSUFBVyxFQUFFLEVBQUU7UUFDbkMsSUFBSSxTQUFTLElBQUksSUFBSSxFQUFFO1lBQ3JCLFNBQVMsR0FBRyxZQUFZLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxDQUFDLENBQUM7U0FDdkM7SUFDSCxDQUFDLENBQUE7SUFFRCxpREFBaUQ7SUFDakQsQ0FBQyxtQkFBQSxTQUFTLEVBQU8sQ0FBQyxDQUFDLE1BQU07OztJQUFHLEdBQUcsRUFBRSxDQUFDLDJCQUEyQixDQUFDLG1CQUFBLFNBQVMsRUFBQyxDQUFDLENBQUEsQ0FBQztJQUUxRSxPQUFPLFNBQVMsQ0FBQztBQUNuQixDQUFDOzs7O0FBRUQsTUFBTSxVQUFVLGlDQUFpQztJQUMvQzs7Ozs7O0lBQU8sVUFBVSxNQUFXLEVBQUUsR0FBVyxFQUFFLFVBQWU7O2NBQ2xELEVBQUUsR0FBRyxVQUFVLENBQUMsS0FBSzs7WUFDdkIsZ0JBQWdCLEdBQUcsS0FBSztRQUM1QixPQUFPO1lBQ0wsWUFBWSxFQUFFLElBQUk7Ozs7WUFDbEIsR0FBRztnQkFDRCxJQUFJLGdCQUFnQixJQUFJLElBQUksS0FBSyxNQUFNLENBQUMsU0FBUyxJQUFJLElBQUksQ0FBQyxjQUFjLENBQUMsR0FBRyxDQUFDLEVBQUU7b0JBQzdFLE9BQU8sRUFBRSxDQUFDO2lCQUNYOztzQkFFSyxPQUFPLEdBQUcsd0JBQXdCLENBQUMsRUFBRSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztnQkFDdkQsZ0JBQWdCLEdBQUcsSUFBSSxDQUFDO2dCQUN4QixNQUFNLENBQUMsY0FBYyxDQUFDLElBQUksRUFBRSxHQUFHLEVBQUU7b0JBQy9CLEtBQUssRUFBUyxPQUFPO29CQUNyQixZQUFZLEVBQUUsSUFBSTtvQkFDbEIsUUFBUSxFQUFNLElBQUk7aUJBQ25CLENBQUMsQ0FBQztnQkFDSCxnQkFBZ0IsR0FBRyxLQUFLLENBQUM7Z0JBQ3pCLE9BQU8sT0FBTyxDQUFDO1lBQ2pCLENBQUM7U0FDRixDQUFDO0lBQ0osQ0FBQyxFQUFDO0FBQ0osQ0FBQyIsInNvdXJjZXNDb250ZW50IjpbIi8vIHRzbGludDpkaXNhYmxlOm5vLWFueSB0eXBlZGVmIG5vLWludmFsaWQtdGhpc1xuaW1wb3J0IHsgY2FuY2VsUmVxdWVzdEFuaW1hdGlvbkZyYW1lLCByZXFBbmltRnJhbWUgfSBmcm9tICcuLi9wb2x5ZmlsbC9yZXF1ZXN0LWFuaW1hdGlvbic7XG5cbmV4cG9ydCBkZWZhdWx0IGZ1bmN0aW9uIHRocm90dGxlQnlBbmltYXRpb25GcmFtZShmbjogYW55KSB7XG4gIGxldCByZXF1ZXN0SWQ6IG51bWJlciB8IG51bGw7XG5cbiAgY29uc3QgbGF0ZXIgPSAoYXJnczogYW55W10pID0+ICgpID0+IHtcbiAgICByZXF1ZXN0SWQgPSBudWxsO1xuICAgIGZuKC4uLmFyZ3MpO1xuICB9O1xuXG4gIGNvbnN0IHRocm90dGxlZCA9ICguLi5hcmdzOiBhbnlbXSkgPT4ge1xuICAgIGlmIChyZXF1ZXN0SWQgPT0gbnVsbCkge1xuICAgICAgcmVxdWVzdElkID0gcmVxQW5pbUZyYW1lKGxhdGVyKGFyZ3MpKTtcbiAgICB9XG4gIH07XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLW5vbi1udWxsLWFzc2VydGlvblxuICAodGhyb3R0bGVkIGFzIGFueSkuY2FuY2VsID0gKCkgPT4gY2FuY2VsUmVxdWVzdEFuaW1hdGlvbkZyYW1lKHJlcXVlc3RJZCEpO1xuXG4gIHJldHVybiB0aHJvdHRsZWQ7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiB0aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWVEZWNvcmF0b3IoKSB7XG4gIHJldHVybiBmdW5jdGlvbiAodGFyZ2V0OiBhbnksIGtleTogc3RyaW5nLCBkZXNjcmlwdG9yOiBhbnkpIHtcbiAgICBjb25zdCBmbiA9IGRlc2NyaXB0b3IudmFsdWU7XG4gICAgbGV0IGRlZmluaW5nUHJvcGVydHkgPSBmYWxzZTtcbiAgICByZXR1cm4ge1xuICAgICAgY29uZmlndXJhYmxlOiB0cnVlLFxuICAgICAgZ2V0KCkge1xuICAgICAgICBpZiAoZGVmaW5pbmdQcm9wZXJ0eSB8fCB0aGlzID09PSB0YXJnZXQucHJvdG90eXBlIHx8IHRoaXMuaGFzT3duUHJvcGVydHkoa2V5KSkge1xuICAgICAgICAgIHJldHVybiBmbjtcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IGJvdW5kRm4gPSB0aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWUoZm4uYmluZCh0aGlzKSk7XG4gICAgICAgIGRlZmluaW5nUHJvcGVydHkgPSB0cnVlO1xuICAgICAgICBPYmplY3QuZGVmaW5lUHJvcGVydHkodGhpcywga2V5LCB7XG4gICAgICAgICAgdmFsdWUgICAgICAgOiBib3VuZEZuLFxuICAgICAgICAgIGNvbmZpZ3VyYWJsZTogdHJ1ZSxcbiAgICAgICAgICB3cml0YWJsZSAgICA6IHRydWVcbiAgICAgICAgfSk7XG4gICAgICAgIGRlZmluaW5nUHJvcGVydHkgPSBmYWxzZTtcbiAgICAgICAgcmV0dXJuIGJvdW5kRm47XG4gICAgICB9XG4gICAgfTtcbiAgfTtcbn1cbiJdfQ==