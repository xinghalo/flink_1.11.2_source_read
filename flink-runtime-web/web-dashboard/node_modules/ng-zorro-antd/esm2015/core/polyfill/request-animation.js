/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
// tslint:disable:no-any typedef no-invalid-this
/** @type {?} */
const availablePrefixs = ['moz', 'ms', 'webkit'];
/**
 * @return {?}
 */
function requestAnimationFramePolyfill() {
    /** @type {?} */
    let lastTime = 0;
    return (/**
     * @param {?} callback
     * @return {?}
     */
    function (callback) {
        /** @type {?} */
        const currTime = new Date().getTime();
        /** @type {?} */
        const timeToCall = Math.max(0, 16 - (currTime - lastTime));
        /** @type {?} */
        const id = setTimeout((/**
         * @return {?}
         */
        () => {
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
        () => 0);
    }
    if (window.requestAnimationFrame) {
        // https://github.com/vuejs/vue/issues/4465
        return window.requestAnimationFrame.bind(window);
    }
    /** @type {?} */
    const prefix = availablePrefixs.filter((/**
     * @param {?} key
     * @return {?}
     */
    key => `${key}RequestAnimationFrame` in window))[0];
    return prefix
        // @ts-ignore
        ? window[`${prefix}RequestAnimationFrame`]
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
    const prefix = availablePrefixs.filter((/**
     * @param {?} key
     * @return {?}
     */
    key => `${key}CancelAnimationFrame` in window || `${key}CancelRequestAnimationFrame` in window))[0];
    return prefix ?
        (((/** @type {?} */ (window)))[`${prefix}CancelAnimationFrame`] ||
            ((/** @type {?} */ (window)))[`${prefix}CancelRequestAnimationFrame`]
        // @ts-ignore
        ).call(this, id) : clearTimeout(id);
}
/** @type {?} */
export const reqAnimFrame = getRequestAnimationFrame();
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicmVxdWVzdC1hbmltYXRpb24uanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS9wb2x5ZmlsbC9yZXF1ZXN0LWFuaW1hdGlvbi50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7TUFDTSxnQkFBZ0IsR0FBRyxDQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsUUFBUSxDQUFFOzs7O0FBRWxELFNBQVMsNkJBQTZCOztRQUNoQyxRQUFRLEdBQUcsQ0FBQztJQUNoQjs7OztJQUFPLFVBQVUsUUFBOEI7O2NBQ3ZDLFFBQVEsR0FBRyxJQUFJLElBQUksRUFBRSxDQUFDLE9BQU8sRUFBRTs7Y0FDL0IsVUFBVSxHQUFHLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxFQUFFLEVBQUUsR0FBRyxDQUFDLFFBQVEsR0FBRyxRQUFRLENBQUMsQ0FBQzs7Y0FDcEQsRUFBRSxHQUFHLFVBQVU7OztRQUFDLEdBQUcsRUFBRTtZQUN6QixRQUFRLENBQUMsUUFBUSxHQUFHLFVBQVUsQ0FBQyxDQUFDO1FBQ2xDLENBQUMsR0FBRSxVQUFVLENBQUM7UUFDZCxRQUFRLEdBQUcsUUFBUSxHQUFHLFVBQVUsQ0FBQztRQUNqQyxPQUFPLEVBQUUsQ0FBQztJQUNaLENBQUMsRUFBQztBQUNKLENBQUM7Ozs7QUFFRCxTQUFTLHdCQUF3QjtJQUMvQixJQUFJLE9BQU8sTUFBTSxLQUFLLFdBQVcsRUFBRTtRQUNqQzs7O1FBQU8sR0FBRyxFQUFFLENBQUMsQ0FBQyxFQUFDO0tBQ2hCO0lBQ0QsSUFBSSxNQUFNLENBQUMscUJBQXFCLEVBQUU7UUFDaEMsMkNBQTJDO1FBQzNDLE9BQU8sTUFBTSxDQUFDLHFCQUFxQixDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQztLQUNsRDs7VUFFSyxNQUFNLEdBQUcsZ0JBQWdCLENBQUMsTUFBTTs7OztJQUFDLEdBQUcsQ0FBQyxFQUFFLENBQUMsR0FBRyxHQUFHLHVCQUF1QixJQUFJLE1BQU0sRUFBQyxDQUFFLENBQUMsQ0FBRTtJQUUzRixPQUFPLE1BQU07UUFDWCxhQUFhO1FBQ2IsQ0FBQyxDQUFDLE1BQU0sQ0FBRSxHQUFHLE1BQU0sdUJBQXVCLENBQUU7UUFDNUMsQ0FBQyxDQUFDLDZCQUE2QixFQUFFLENBQUM7QUFDdEMsQ0FBQzs7Ozs7QUFFRCxNQUFNLFVBQVUsMkJBQTJCLENBQUMsRUFBVTtJQUNwRCxJQUFJLE9BQU8sTUFBTSxLQUFLLFdBQVcsRUFBRTtRQUNqQyxPQUFPLElBQUksQ0FBQztLQUNiO0lBQ0QsSUFBSSxNQUFNLENBQUMsb0JBQW9CLEVBQUU7UUFDL0IsT0FBTyxNQUFNLENBQUMsb0JBQW9CLENBQUMsRUFBRSxDQUFDLENBQUM7S0FDeEM7O1VBQ0ssTUFBTSxHQUFHLGdCQUFnQixDQUFDLE1BQU07Ozs7SUFBQyxHQUFHLENBQUMsRUFBRSxDQUMzQyxHQUFHLEdBQUcsc0JBQXNCLElBQUksTUFBTSxJQUFJLEdBQUcsR0FBRyw2QkFBNkIsSUFBSSxNQUFNLEVBQ3hGLENBQUUsQ0FBQyxDQUFFO0lBRU4sT0FBTyxNQUFNLENBQUMsQ0FBQztRQUNiLENBQ0UsQ0FBQyxtQkFBQSxNQUFNLEVBQU8sQ0FBQyxDQUFFLEdBQUcsTUFBTSxzQkFBc0IsQ0FBRTtZQUNsRCxDQUFDLG1CQUFBLE1BQU0sRUFBTyxDQUFDLENBQUUsR0FBRyxNQUFNLDZCQUE2QixDQUFFO1FBQ3pELGFBQWE7U0FDZCxDQUFDLElBQUksQ0FBQyxJQUFJLEVBQUUsRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFDLFlBQVksQ0FBQyxFQUFFLENBQUMsQ0FBQztBQUN4QyxDQUFDOztBQUVELE1BQU0sT0FBTyxZQUFZLEdBQUcsd0JBQXdCLEVBQUUiLCJzb3VyY2VzQ29udGVudCI6WyIvLyB0c2xpbnQ6ZGlzYWJsZTpuby1hbnkgdHlwZWRlZiBuby1pbnZhbGlkLXRoaXNcbmNvbnN0IGF2YWlsYWJsZVByZWZpeHMgPSBbICdtb3onLCAnbXMnLCAnd2Via2l0JyBdO1xuXG5mdW5jdGlvbiByZXF1ZXN0QW5pbWF0aW9uRnJhbWVQb2x5ZmlsbCgpOiB0eXBlb2YgcmVxdWVzdEFuaW1hdGlvbkZyYW1lIHtcbiAgbGV0IGxhc3RUaW1lID0gMDtcbiAgcmV0dXJuIGZ1bmN0aW9uIChjYWxsYmFjazogRnJhbWVSZXF1ZXN0Q2FsbGJhY2spOiBudW1iZXIge1xuICAgIGNvbnN0IGN1cnJUaW1lID0gbmV3IERhdGUoKS5nZXRUaW1lKCk7XG4gICAgY29uc3QgdGltZVRvQ2FsbCA9IE1hdGgubWF4KDAsIDE2IC0gKGN1cnJUaW1lIC0gbGFzdFRpbWUpKTtcbiAgICBjb25zdCBpZCA9IHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgY2FsbGJhY2soY3VyclRpbWUgKyB0aW1lVG9DYWxsKTtcbiAgICB9LCB0aW1lVG9DYWxsKTtcbiAgICBsYXN0VGltZSA9IGN1cnJUaW1lICsgdGltZVRvQ2FsbDtcbiAgICByZXR1cm4gaWQ7XG4gIH07XG59XG5cbmZ1bmN0aW9uIGdldFJlcXVlc3RBbmltYXRpb25GcmFtZSgpOiB0eXBlb2YgcmVxdWVzdEFuaW1hdGlvbkZyYW1lIHtcbiAgaWYgKHR5cGVvZiB3aW5kb3cgPT09ICd1bmRlZmluZWQnKSB7XG4gICAgcmV0dXJuICgpID0+IDA7XG4gIH1cbiAgaWYgKHdpbmRvdy5yZXF1ZXN0QW5pbWF0aW9uRnJhbWUpIHtcbiAgICAvLyBodHRwczovL2dpdGh1Yi5jb20vdnVlanMvdnVlL2lzc3Vlcy80NDY1XG4gICAgcmV0dXJuIHdpbmRvdy5yZXF1ZXN0QW5pbWF0aW9uRnJhbWUuYmluZCh3aW5kb3cpO1xuICB9XG5cbiAgY29uc3QgcHJlZml4ID0gYXZhaWxhYmxlUHJlZml4cy5maWx0ZXIoa2V5ID0+IGAke2tleX1SZXF1ZXN0QW5pbWF0aW9uRnJhbWVgIGluIHdpbmRvdylbIDAgXTtcblxuICByZXR1cm4gcHJlZml4XG4gICAgLy8gQHRzLWlnbm9yZVxuICAgID8gd2luZG93WyBgJHtwcmVmaXh9UmVxdWVzdEFuaW1hdGlvbkZyYW1lYCBdXG4gICAgOiByZXF1ZXN0QW5pbWF0aW9uRnJhbWVQb2x5ZmlsbCgpO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gY2FuY2VsUmVxdWVzdEFuaW1hdGlvbkZyYW1lKGlkOiBudW1iZXIpOiBhbnkge1xuICBpZiAodHlwZW9mIHdpbmRvdyA9PT0gJ3VuZGVmaW5lZCcpIHtcbiAgICByZXR1cm4gbnVsbDtcbiAgfVxuICBpZiAod2luZG93LmNhbmNlbEFuaW1hdGlvbkZyYW1lKSB7XG4gICAgcmV0dXJuIHdpbmRvdy5jYW5jZWxBbmltYXRpb25GcmFtZShpZCk7XG4gIH1cbiAgY29uc3QgcHJlZml4ID0gYXZhaWxhYmxlUHJlZml4cy5maWx0ZXIoa2V5ID0+XG4gICAgYCR7a2V5fUNhbmNlbEFuaW1hdGlvbkZyYW1lYCBpbiB3aW5kb3cgfHwgYCR7a2V5fUNhbmNlbFJlcXVlc3RBbmltYXRpb25GcmFtZWAgaW4gd2luZG93XG4gIClbIDAgXTtcblxuICByZXR1cm4gcHJlZml4ID9cbiAgICAoXG4gICAgICAod2luZG93IGFzIGFueSlbIGAke3ByZWZpeH1DYW5jZWxBbmltYXRpb25GcmFtZWAgXSB8fFxuICAgICAgKHdpbmRvdyBhcyBhbnkpWyBgJHtwcmVmaXh9Q2FuY2VsUmVxdWVzdEFuaW1hdGlvbkZyYW1lYCBdXG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgKS5jYWxsKHRoaXMsIGlkKSA6IGNsZWFyVGltZW91dChpZCk7XG59XG5cbmV4cG9ydCBjb25zdCByZXFBbmltRnJhbWUgPSBnZXRSZXF1ZXN0QW5pbWF0aW9uRnJhbWUoKTtcbiJdfQ==