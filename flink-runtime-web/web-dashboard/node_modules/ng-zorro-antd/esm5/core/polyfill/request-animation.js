/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
// tslint:disable:no-any typedef no-invalid-this
/** @type {?} */
var availablePrefixs = ['moz', 'ms', 'webkit'];
/**
 * @return {?}
 */
function requestAnimationFramePolyfill() {
    /** @type {?} */
    var lastTime = 0;
    return (/**
     * @param {?} callback
     * @return {?}
     */
    function (callback) {
        /** @type {?} */
        var currTime = new Date().getTime();
        /** @type {?} */
        var timeToCall = Math.max(0, 16 - (currTime - lastTime));
        /** @type {?} */
        var id = setTimeout((/**
         * @return {?}
         */
        function () {
            callback(currTime + timeToCall);
        }), timeToCall);
        lastTime = currTime + timeToCall;
        return id;
    });
}
/**
 * @return {?}
 */
function getRequestAnimationFrame() {
    if (typeof window === 'undefined') {
        return (/**
         * @return {?}
         */
        function () { return 0; });
    }
    if (window.requestAnimationFrame) {
        // https://github.com/vuejs/vue/issues/4465
        return window.requestAnimationFrame.bind(window);
    }
    /** @type {?} */
    var prefix = availablePrefixs.filter((/**
     * @param {?} key
     * @return {?}
     */
    function (key) { return key + "RequestAnimationFrame" in window; }))[0];
    return prefix
        // @ts-ignore
        ? window[prefix + "RequestAnimationFrame"]
        : requestAnimationFramePolyfill();
}
/**
 * @param {?} id
 * @return {?}
 */
export function cancelRequestAnimationFrame(id) {
    if (typeof window === 'undefined') {
        return null;
    }
    if (window.cancelAnimationFrame) {
        return window.cancelAnimationFrame(id);
    }
    /** @type {?} */
    var prefix = availablePrefixs.filter((/**
     * @param {?} key
     * @return {?}
     */
    function (key) {
        return key + "CancelAnimationFrame" in window || key + "CancelRequestAnimationFrame" in window;
    }))[0];
    return prefix ?
        (((/** @type {?} */ (window)))[prefix + "CancelAnimationFrame"] ||
            ((/** @type {?} */ (window)))[prefix + "CancelRequestAnimationFrame"]
        // @ts-ignore
        ).call(this, id) : clearTimeout(id);
}
/** @type {?} */
export var reqAnimFrame = getRequestAnimationFrame();
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVxdWVzdC1hbmltYXRpb24uanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS9wb2x5ZmlsbC9yZXF1ZXN0LWFuaW1hdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7SUFDTSxnQkFBZ0IsR0FBRyxDQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsUUFBUSxDQUFFOzs7O0FBRWxELFNBQVMsNkJBQTZCOztRQUNoQyxRQUFRLEdBQUcsQ0FBQztJQUNoQjs7OztJQUFPLFVBQVUsUUFBOEI7O1lBQ3ZDLFFBQVEsR0FBRyxJQUFJLElBQUksRUFBRSxDQUFDLE9BQU8sRUFBRTs7WUFDL0IsVUFBVSxHQUFHLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxFQUFFLEVBQUUsR0FBRyxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUMsQ0FBQzs7WUFDcEQsRUFBRSxHQUFHLFVBQVU7OztRQUFDO1lBQ3BCLFFBQVEsQ0FBQyxRQUFRLEdBQUcsVUFBVSxDQUFDLENBQUM7UUFDbEMsQ0FBQyxHQUFFLFVBQVUsQ0FBQztRQUNkLFFBQVEsR0FBRyxRQUFRLEdBQUcsVUFBVSxDQUFDO1FBQ2pDLE9BQU8sRUFBRSxDQUFDO0lBQ1osQ0FBQyxFQUFDO0FBQ0osQ0FBQzs7OztBQUVELFNBQVMsd0JBQXdCO0lBQy9CLElBQUksT0FBTyxNQUFNLEtBQUssV0FBVyxFQUFFO1FBQ2pDOzs7UUFBTyxjQUFNLE9BQUEsQ0FBQyxFQUFELENBQUMsRUFBQztLQUNoQjtJQUNELElBQUksTUFBTSxDQUFDLHFCQUFxQixFQUFFO1FBQ2hDLDJDQUEyQztRQUMzQyxPQUFPLE1BQU0sQ0FBQyxxQkFBcUIsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLENBQUM7S0FDbEQ7O1FBRUssTUFBTSxHQUFHLGdCQUFnQixDQUFDLE1BQU07Ozs7SUFBQyxVQUFBLEdBQUcsSUFBSSxPQUFHLEdBQUcsMEJBQXVCLElBQUksTUFBTSxFQUF2QyxDQUF1QyxFQUFDLENBQUUsQ0FBQyxDQUFFO0lBRTNGLE9BQU8sTUFBTTtRQUNYLGFBQWE7UUFDYixDQUFDLENBQUMsTUFBTSxDQUFLLE1BQU0sMEJBQXVCLENBQUU7UUFDNUMsQ0FBQyxDQUFDLDZCQUE2QixFQUFFLENBQUM7QUFDdEMsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsMkJBQTJCLENBQUMsRUFBVTtJQUNwRCxJQUFJLE9BQU8sTUFBTSxLQUFLLFdBQVcsRUFBRTtRQUNqQyxPQUFPLElBQUksQ0FBQztLQUNiO0lBQ0QsSUFBSSxNQUFNLENBQUMsb0JBQW9CLEVBQUU7UUFDL0IsT0FBTyxNQUFNLENBQUMsb0JBQW9CLENBQUMsRUFBRSxDQUFDLENBQUM7S0FDeEM7O1FBQ0ssTUFBTSxHQUFHLGdCQUFnQixDQUFDLE1BQU07Ozs7SUFBQyxVQUFBLEdBQUc7UUFDeEMsT0FBRyxHQUFHLHlCQUFzQixJQUFJLE1BQU0sSUFBTyxHQUFHLGdDQUE2QixJQUFJLE1BQU07SUFBdkYsQ0FBdUYsRUFDeEYsQ0FBRSxDQUFDLENBQUU7SUFFTixPQUFPLE1BQU0sQ0FBQyxDQUFDO1FBQ2IsQ0FDRSxDQUFDLG1CQUFBLE1BQU0sRUFBTyxDQUFDLENBQUssTUFBTSx5QkFBc0IsQ0FBRTtZQUNsRCxDQUFDLG1CQUFBLE1BQU0sRUFBTyxDQUFDLENBQUssTUFBTSxnQ0FBNkIsQ0FBRTtRQUN6RCxhQUFhO1NBQ2QsQ0FBQyxJQUFJLENBQUMsSUFBSSxFQUFFLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBQyxZQUFZLENBQUMsRUFBRSxDQUFDLENBQUM7QUFDeEMsQ0FBQzs7QUFFRCxNQUFNLEtBQU8sWUFBWSxHQUFHLHdCQUF3QixFQUFFIiwic291cmNlc0NvbnRlbnQiOlsiLy8gdHNsaW50OmRpc2FibGU6bm8tYW55IHR5cGVkZWYgbm8taW52YWxpZC10aGlzXG5jb25zdCBhdmFpbGFibGVQcmVmaXhzID0gWyAnbW96JywgJ21zJywgJ3dlYmtpdCcgXTtcblxuZnVuY3Rpb24gcmVxdWVzdEFuaW1hdGlvbkZyYW1lUG9seWZpbGwoKTogdHlwZW9mIHJlcXVlc3RBbmltYXRpb25GcmFtZSB7XG4gIGxldCBsYXN0VGltZSA9IDA7XG4gIHJldHVybiBmdW5jdGlvbiAoY2FsbGJhY2s6IEZyYW1lUmVxdWVzdENhbGxiYWNrKTogbnVtYmVyIHtcbiAgICBjb25zdCBjdXJyVGltZSA9IG5ldyBEYXRlKCkuZ2V0VGltZSgpO1xuICAgIGNvbnN0IHRpbWVUb0NhbGwgPSBNYXRoLm1heCgwLCAxNiAtIChjdXJyVGltZSAtIGxhc3RUaW1lKSk7XG4gICAgY29uc3QgaWQgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIGNhbGxiYWNrKGN1cnJUaW1lICsgdGltZVRvQ2FsbCk7XG4gICAgfSwgdGltZVRvQ2FsbCk7XG4gICAgbGFzdFRpbWUgPSBjdXJyVGltZSArIHRpbWVUb0NhbGw7XG4gICAgcmV0dXJuIGlkO1xuICB9O1xufVxuXG5mdW5jdGlvbiBnZXRSZXF1ZXN0QW5pbWF0aW9uRnJhbWUoKTogdHlwZW9mIHJlcXVlc3RBbmltYXRpb25GcmFtZSB7XG4gIGlmICh0eXBlb2Ygd2luZG93ID09PSAndW5kZWZpbmVkJykge1xuICAgIHJldHVybiAoKSA9PiAwO1xuICB9XG4gIGlmICh3aW5kb3cucmVxdWVzdEFuaW1hdGlvbkZyYW1lKSB7XG4gICAgLy8gaHR0cHM6Ly9naXRodWIuY29tL3Z1ZWpzL3Z1ZS9pc3N1ZXMvNDQ2NVxuICAgIHJldHVybiB3aW5kb3cucmVxdWVzdEFuaW1hdGlvbkZyYW1lLmJpbmQod2luZG93KTtcbiAgfVxuXG4gIGNvbnN0IHByZWZpeCA9IGF2YWlsYWJsZVByZWZpeHMuZmlsdGVyKGtleSA9PiBgJHtrZXl9UmVxdWVzdEFuaW1hdGlvbkZyYW1lYCBpbiB3aW5kb3cpWyAwIF07XG5cbiAgcmV0dXJuIHByZWZpeFxuICAgIC8vIEB0cy1pZ25vcmVcbiAgICA/IHdpbmRvd1sgYCR7cHJlZml4fVJlcXVlc3RBbmltYXRpb25GcmFtZWAgXVxuICAgIDogcmVxdWVzdEFuaW1hdGlvbkZyYW1lUG9seWZpbGwoKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGNhbmNlbFJlcXVlc3RBbmltYXRpb25GcmFtZShpZDogbnVtYmVyKTogYW55IHtcbiAgaWYgKHR5cGVvZiB3aW5kb3cgPT09ICd1bmRlZmluZWQnKSB7XG4gICAgcmV0dXJuIG51bGw7XG4gIH1cbiAgaWYgKHdpbmRvdy5jYW5jZWxBbmltYXRpb25GcmFtZSkge1xuICAgIHJldHVybiB3aW5kb3cuY2FuY2VsQW5pbWF0aW9uRnJhbWUoaWQpO1xuICB9XG4gIGNvbnN0IHByZWZpeCA9IGF2YWlsYWJsZVByZWZpeHMuZmlsdGVyKGtleSA9PlxuICAgIGAke2tleX1DYW5jZWxBbmltYXRpb25GcmFtZWAgaW4gd2luZG93IHx8IGAke2tleX1DYW5jZWxSZXF1ZXN0QW5pbWF0aW9uRnJhbWVgIGluIHdpbmRvd1xuICApWyAwIF07XG5cbiAgcmV0dXJuIHByZWZpeCA/XG4gICAgKFxuICAgICAgKHdpbmRvdyBhcyBhbnkpWyBgJHtwcmVmaXh9Q2FuY2VsQW5pbWF0aW9uRnJhbWVgIF0gfHxcbiAgICAgICh3aW5kb3cgYXMgYW55KVsgYCR7cHJlZml4fUNhbmNlbFJlcXVlc3RBbmltYXRpb25GcmFtZWAgXVxuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICkuY2FsbCh0aGlzLCBpZCkgOiBjbGVhclRpbWVvdXQoaWQpO1xufVxuXG5leHBvcnQgY29uc3QgcmVxQW5pbUZyYW1lID0gZ2V0UmVxdWVzdEFuaW1hdGlvbkZyYW1lKCk7XG4iXX0=