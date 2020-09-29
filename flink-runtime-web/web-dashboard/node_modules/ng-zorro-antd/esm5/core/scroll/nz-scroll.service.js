/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { DOCUMENT } from '@angular/common';
import { Inject, Injectable, Optional, SkipSelf } from '@angular/core';
import { reqAnimFrame } from '../polyfill/request-animation';
/**
 * @param {?} t
 * @param {?} b
 * @param {?} c
 * @param {?} d
 * @return {?}
 */
function easeInOutCubic(t, b, c, d) {
    /** @type {?} */
    var cc = c - b;
    /** @type {?} */
    var tt = t / (d / 2);
    if (tt < 1) {
        return cc / 2 * tt * tt * tt + b;
    }
    else {
        return cc / 2 * ((tt -= 2) * tt * tt + 2) + b;
    }
}
var NzScrollService = /** @class */ (function () {
    /* tslint:disable-next-line:no-any */
    function NzScrollService(doc) {
        this.doc = doc;
    }
    /** 设置 `el` 滚动条位置 */
    /**
     * 设置 `el` 滚动条位置
     * @param {?} el
     * @param {?=} topValue
     * @return {?}
     */
    NzScrollService.prototype.setScrollTop = /**
     * 设置 `el` 滚动条位置
     * @param {?} el
     * @param {?=} topValue
     * @return {?}
     */
    function (el, topValue) {
        if (topValue === void 0) { topValue = 0; }
        if (el === window) {
            this.doc.body.scrollTop = topValue;
            (/** @type {?} */ (this.doc.documentElement)).scrollTop = topValue;
        }
        else {
            ((/** @type {?} */ (el))).scrollTop = topValue;
        }
    };
    /** 获取 `el` 相对于视窗距离 */
    /**
     * 获取 `el` 相对于视窗距离
     * @param {?} el
     * @return {?}
     */
    NzScrollService.prototype.getOffset = /**
     * 获取 `el` 相对于视窗距离
     * @param {?} el
     * @return {?}
     */
    function (el) {
        /** @type {?} */
        var ret = {
            top: 0,
            left: 0
        };
        if (!el || !el.getClientRects().length) {
            return ret;
        }
        /** @type {?} */
        var rect = el.getBoundingClientRect();
        if (rect.width || rect.height) {
            /** @type {?} */
            var doc = (/** @type {?} */ (el.ownerDocument)).documentElement;
            ret.top = rect.top - (/** @type {?} */ (doc)).clientTop;
            ret.left = rect.left - (/** @type {?} */ (doc)).clientLeft;
        }
        else {
            ret.top = rect.top;
            ret.left = rect.left;
        }
        return ret;
    };
    /** 获取 `el` 滚动条位置 */
    // TODO: remove '| Window' as the fallback already happens here
    /**
     * 获取 `el` 滚动条位置
     * @param {?=} el
     * @param {?=} top
     * @return {?}
     */
    // TODO: remove '| Window' as the fallback already happens here
    NzScrollService.prototype.getScroll = /**
     * 获取 `el` 滚动条位置
     * @param {?=} el
     * @param {?=} top
     * @return {?}
     */
    // TODO: remove '| Window' as the fallback already happens here
    function (el, top) {
        if (top === void 0) { top = true; }
        /** @type {?} */
        var target = el ? el : window;
        /** @type {?} */
        var prop = top ? 'pageYOffset' : 'pageXOffset';
        /** @type {?} */
        var method = top ? 'scrollTop' : 'scrollLeft';
        /** @type {?} */
        var isWindow = target === window;
        // @ts-ignore
        /** @type {?} */
        var ret = isWindow ? target[prop] : target[method];
        if (isWindow && typeof ret !== 'number') {
            ret = (/** @type {?} */ (this.doc.documentElement))[method];
        }
        return ret;
    };
    /**
     * 使用动画形式将 `el` 滚动至某位置
     *
     * @param containerEl 容器，默认 `window`
     * @param targetTopValue 滚动至目标 `top` 值，默认：0，相当于顶部
     * @param easing 动作算法，默认：`easeInOutCubic`
     * @param callback 动画结束后回调
     */
    /**
     * 使用动画形式将 `el` 滚动至某位置
     *
     * @param {?} containerEl 容器，默认 `window`
     * @param {?=} targetTopValue 滚动至目标 `top` 值，默认：0，相当于顶部
     * @param {?=} easing 动作算法，默认：`easeInOutCubic`
     * @param {?=} callback 动画结束后回调
     * @return {?}
     */
    NzScrollService.prototype.scrollTo = /**
     * 使用动画形式将 `el` 滚动至某位置
     *
     * @param {?} containerEl 容器，默认 `window`
     * @param {?=} targetTopValue 滚动至目标 `top` 值，默认：0，相当于顶部
     * @param {?=} easing 动作算法，默认：`easeInOutCubic`
     * @param {?=} callback 动画结束后回调
     * @return {?}
     */
    function (containerEl, targetTopValue, easing, callback) {
        var _this = this;
        if (targetTopValue === void 0) { targetTopValue = 0; }
        /** @type {?} */
        var target = containerEl ? containerEl : window;
        /** @type {?} */
        var scrollTop = this.getScroll(target);
        /** @type {?} */
        var startTime = Date.now();
        /** @type {?} */
        var frameFunc = (/**
         * @return {?}
         */
        function () {
            /** @type {?} */
            var timestamp = Date.now();
            /** @type {?} */
            var time = timestamp - startTime;
            _this.setScrollTop(target, (easing || easeInOutCubic)(time, scrollTop, targetTopValue, 450));
            if (time < 450) {
                reqAnimFrame(frameFunc);
            }
            else {
                if (callback) {
                    callback();
                }
            }
        });
        reqAnimFrame(frameFunc);
    };
    NzScrollService.decorators = [
        { type: Injectable }
    ];
    /** @nocollapse */
    NzScrollService.ctorParameters = function () { return [
        { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
    ]; };
    return NzScrollService;
}());
export { NzScrollService };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzScrollService.prototype.doc;
}
/**
 * @param {?} doc
 * @param {?} scrollService
 * @return {?}
 */
export function SCROLL_SERVICE_PROVIDER_FACTORY(doc, scrollService) {
    return scrollService || new NzScrollService(doc);
}
/** @type {?} */
export var SCROLL_SERVICE_PROVIDER = {
    provide: NzScrollService,
    useFactory: SCROLL_SERVICE_PROVIDER_FACTORY,
    deps: [DOCUMENT, [new Optional(), new SkipSelf(), NzScrollService]]
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2Nyb2xsLnNlcnZpY2UuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS9zY3JvbGwvbnotc2Nyb2xsLnNlcnZpY2UudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMzQyxPQUFPLEVBQUUsTUFBTSxFQUFFLFVBQVUsRUFBRSxRQUFRLEVBQVksUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRWpGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSwrQkFBK0IsQ0FBQzs7Ozs7Ozs7QUFJN0QsU0FBUyxjQUFjLENBQUMsQ0FBUyxFQUFFLENBQVMsRUFBRSxDQUFTLEVBQUUsQ0FBUzs7UUFDMUQsRUFBRSxHQUFHLENBQUMsR0FBRyxDQUFDOztRQUNaLEVBQUUsR0FBRyxDQUFDLEdBQUcsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQ3BCLElBQUksRUFBRSxHQUFHLENBQUMsRUFBRTtRQUNWLE9BQU8sRUFBRSxHQUFHLENBQUMsR0FBRyxFQUFFLEdBQUcsRUFBRSxHQUFHLEVBQUUsR0FBRyxDQUFDLENBQUM7S0FDbEM7U0FBTTtRQUNMLE9BQU8sRUFBRSxHQUFHLENBQUMsR0FBRyxDQUFDLENBQUMsRUFBRSxJQUFJLENBQUMsQ0FBQyxHQUFHLEVBQUUsR0FBRyxFQUFFLEdBQUcsQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDO0tBQy9DO0FBQ0gsQ0FBQztBQUVEO0lBSUUscUNBQXFDO0lBQ3JDLHlCQUE4QixHQUFRO1FBQ3BDLElBQUksQ0FBQyxHQUFHLEdBQUcsR0FBRyxDQUFDO0lBQ2pCLENBQUM7SUFFRCxvQkFBb0I7Ozs7Ozs7SUFDcEIsc0NBQVk7Ozs7OztJQUFaLFVBQWEsRUFBb0IsRUFBRSxRQUFvQjtRQUFwQix5QkFBQSxFQUFBLFlBQW9CO1FBQ3JELElBQUksRUFBRSxLQUFLLE1BQU0sRUFBRTtZQUNqQixJQUFJ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Q0FBQyxNQUFNLElBQUksY0FBYyxDQUFDLENBQUMsSUFBSSxFQUFFLFNBQVMsRUFBRSxjQUFjLEVBQUUsR0FBRyxDQUFDLENBQUMsQ0FBQztZQUM1RixJQUFJLElBQUksR0FBRyxHQUFHLEVBQUU7Z0JBQ2QsWUFBWSxDQUFDLFNBQVMsQ0FBQyxDQUFDO2FBQ3pCO2lCQUFNO2dCQUNMLElBQUksUUFBUSxFQUFFO29CQUFFLFFBQVEsRUFBRSxDQUFDO2lCQUFFO2FBQzlCO1FBQ0gsQ0FBQyxDQUFBO1FBQ0QsWUFBWSxDQUFDLFNBQVMsQ0FBQyxDQUFDO0lBQzFCLENBQUM7O2dCQW5GRixVQUFVOzs7O2dEQUtJLE1BQU0sU0FBQyxRQUFROztJQWdGOUIsc0JBQUM7Q0FBQSxBQXJGRCxJQXFGQztTQXBGWSxlQUFlOzs7Ozs7SUFDMUIsOEJBQXNCOzs7Ozs7O0FBcUZ4QixNQUFNLFVBQVUsK0JBQStCLENBQUMsR0FBYSxFQUFFLGFBQThCO0lBQzNGLE9BQU8sYUFBYSxJQUFJLElBQUksZUFBZSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0FBQ25ELENBQUM7O0FBRUQsTUFBTSxLQUFPLHVCQUF1QixHQUFhO0lBQy9DLE9BQU8sRUFBSyxlQUFlO0lBQzNCLFVBQVUsRUFBRSwrQkFBK0I7SUFDM0MsSUFBSSxFQUFRLENBQUUsUUFBUSxFQUFFLENBQUUsSUFBSSxRQUFRLEVBQUUsRUFBRSxJQUFJLFFBQVEsRUFBRSxFQUFFLGVBQWUsQ0FBRSxDQUFFO0NBQzlFIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRE9DVU1FTlQgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgSW5qZWN0LCBJbmplY3RhYmxlLCBPcHRpb25hbCwgUHJvdmlkZXIsIFNraXBTZWxmIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IHJlcUFuaW1GcmFtZSB9IGZyb20gJy4uL3BvbHlmaWxsL3JlcXVlc3QtYW5pbWF0aW9uJztcblxuZXhwb3J0IHR5cGUgRWFzeWluZ0ZuID0gKHQ6IG51bWJlciwgYjogbnVtYmVyLCBjOiBudW1iZXIsIGQ6IG51bWJlcikgPT4gbnVtYmVyO1xuXG5mdW5jdGlvbiBlYXNlSW5PdXRDdWJpYyh0OiBudW1iZXIsIGI6IG51bWJlciwgYzogbnVtYmVyLCBkOiBudW1iZXIpOiBudW1iZXIge1xuICBjb25zdCBjYyA9IGMgLSBiO1xuICBsZXQgdHQgPSB0IC8gKGQgLyAyKTtcbiAgaWYgKHR0IDwgMSkge1xuICAgIHJldHVybiBjYyAvIDIgKiB0dCAqIHR0ICogdHQgKyBiO1xuICB9IGVsc2Uge1xuICAgIHJldHVybiBjYyAvIDIgKiAoKHR0IC09IDIpICogdHQgKiB0dCArIDIpICsgYjtcbiAgfVxufVxuXG5ASW5qZWN0YWJsZSgpXG5leHBvcnQgY2xhc3MgTnpTY3JvbGxTZXJ2aWNlIHtcbiAgcHJpdmF0ZSBkb2M6IERvY3VtZW50O1xuXG4gIC8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnkgKi9cbiAgY29uc3RydWN0b3IoQEluamVjdChET0NVTUVOVCkgZG9jOiBhbnkpIHtcbiAgICB0aGlzLmRvYyA9IGRvYztcbiAgfVxuXG4gIC8qKiDorr7nva4gYGVsYCDmu5rliqjmnaHkvY3nva4gKi9cbiAgc2V0U2Nyb2xsVG9wKGVsOiBFbGVtZW50IHwgV2luZG93LCB0b3BWYWx1ZTogbnVtYmVyID0gMCk6IHZvaWQge1xuICAgIGlmIChlbCA9PT0gd2luZG93KSB7XG4gICAgICB0aGlzLmRvYy5ib2R5LnNjcm9sbFRvcCA9IHRvcFZhbHVlO1xuICAgICAgdGhpcy5kb2MuZG9jdW1lbnRFbGVtZW50IS5zY3JvbGxUb3AgPSB0b3BWYWx1ZTtcbiAgICB9IGVsc2Uge1xuICAgICAgKGVsIGFzIEVsZW1lbnQpLnNjcm9sbFRvcCA9IHRvcFZhbHVlO1xuICAgIH1cbiAgfVxuXG4gIC8qKiDojrflj5YgYGVsYCDnm7jlr7nkuo7op4bnqpfot53nprsgKi9cbiAgZ2V0T2Zmc2V0KGVsOiBFbGVtZW50KTogeyB0b3A6IG51bWJlciwgbGVmdDogbnVtYmVyIH0ge1xuICAgIGNvbnN0IHJldCA9IHtcbiAgICAgIHRvcCA6IDAsXG4gICAgICBsZWZ0OiAwXG4gICAgfTtcbiAgICBpZiAoIWVsIHx8ICFlbC5nZXRDbGllbnRSZWN0cygpLmxlbmd0aCkgeyByZXR1cm4gcmV0OyB9XG5cbiAgICBjb25zdCByZWN0ID0gZWwuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCk7XG4gICAgaWYgKHJlY3Qud2lkdGggfHwgcmVjdC5oZWlnaHQpIHtcbiAgICAgIGNvbnN0IGRvYyA9IGVsLm93bmVyRG9jdW1lbnQhLmRvY3VtZW50RWxlbWVudDtcbiAgICAgIHJldC50b3AgPSByZWN0LnRvcCAtIGRvYyEuY2xpZW50VG9wO1xuICAgICAgcmV0LmxlZnQgPSByZWN0LmxlZnQgLSBkb2MhLmNsaWVudExlZnQ7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldC50b3AgPSByZWN0LnRvcDtcbiAgICAgIHJldC5sZWZ0ID0gcmVjdC5sZWZ0O1xuICAgIH1cblxuICAgIHJldHVybiByZXQ7XG4gIH1cblxuICAvKiog6I635Y+WIGBlbGAg5rua5Yqo5p2h5L2N572uICovXG4gIC8vIFRPRE86IHJlbW92ZSAnfCBXaW5kb3cnIGFzIHRoZSBmYWxsYmFjayBhbHJlYWR5IGhhcHBlbnMgaGVyZVxuICBnZXRTY3JvbGwoZWw/OiBFbGVtZW50IHwgV2luZG93LCB0b3A6IGJvb2xlYW4gPSB0cnVlKTogbnVtYmVyIHtcbiAgICBjb25zdCB0YXJnZXQgPSBlbCA/IGVsIDogd2luZG93O1xuICAgIGNvbnN0IHByb3AgPSB0b3AgPyAncGFnZVlPZmZzZXQnIDogJ3BhZ2VYT2Zmc2V0JztcbiAgICBjb25zdCBtZXRob2QgPSB0b3AgPyAnc2Nyb2xsVG9wJyA6ICdzY3JvbGxMZWZ0JztcbiAgICBjb25zdCBpc1dpbmRvdyA9IHRhcmdldCA9PT0gd2luZG93O1xuICAgIC8vIEB0cy1pZ25vcmVcbiAgICBsZXQgcmV0ID0gaXNXaW5kb3cgPyB0YXJnZXRbIHByb3AgXSA6IHRhcmdldFsgbWV0aG9kIF07XG4gICAgaWYgKGlzV2luZG93ICYmIHR5cGVvZiByZXQgIT09ICdudW1iZXInKSB7XG4gICAgICByZXQgPSB0aGlzLmRvYy5kb2N1bWVudEVsZW1lbnQhWyBtZXRob2QgXTtcbiAgICB9XG4gICAgcmV0dXJuIHJldDtcbiAgfVxuXG4gIC8qKlxuICAgKiDkvb/nlKjliqjnlLvlvaLlvI/lsIYgYGVsYCDmu5rliqjoh7Pmn5DkvY3nva5cbiAgICpcbiAgICogQHBhcmFtIGNvbnRhaW5lckVsIOWuueWZqO+8jOm7mOiupCBgd2luZG93YFxuICAgKiBAcGFyYW0gdGFyZ2V0VG9wVmFsdWUg5rua5Yqo6Iez55uu5qCHIGB0b3BgIOWAvO+8jOm7mOiupO+8mjDvvIznm7jlvZPkuo7pobbpg6hcbiAgICogQHBhcmFtIGVhc2luZyDliqjkvZznrpfms5XvvIzpu5jorqTvvJpgZWFzZUluT3V0Q3ViaWNgXG4gICAqIEBwYXJhbSBjYWxsYmFjayDliqjnlLvnu5PmnZ/lkI7lm57osINcbiAgICovXG4gIHNjcm9sbFRvKFxuICAgIGNvbnRhaW5lckVsOiBFbGVtZW50IHwgV2luZG93LFxuICAgIHRhcmdldFRvcFZhbHVlOiBudW1iZXIgPSAwLFxuICAgIGVhc2luZz86IEVhc3lpbmdGbixcbiAgICBjYWxsYmFjaz86ICgpID0+IHZvaWRcbiAgKTogdm9pZCB7XG4gICAgY29uc3QgdGFyZ2V0ID0gY29udGFpbmVyRWwgPyBjb250YWluZXJFbCA6IHdpbmRvdztcbiAgICBjb25zdCBzY3JvbGxUb3AgPSB0aGlzLmdldFNjcm9sbCh0YXJnZXQpO1xuICAgIGNvbnN0IHN0YXJ0VGltZSA9IERhdGUubm93KCk7XG4gICAgY29uc3QgZnJhbWVGdW5jID0gKCkgPT4ge1xuICAgICAgY29uc3QgdGltZXN0YW1wID0gRGF0ZS5ub3coKTtcbiAgICAgIGNvbnN0IHRpbWUgPSB0aW1lc3RhbXAgLSBzdGFydFRpbWU7XG4gICAgICB0aGlzLnNldFNjcm9sbFRvcCh0YXJnZXQsIChlYXNpbmcgfHwgZWFzZUluT3V0Q3ViaWMpKHRpbWUsIHNjcm9sbFRvcCwgdGFyZ2V0VG9wVmFsdWUsIDQ1MCkpO1xuICAgICAgaWYgKHRpbWUgPCA0NTApIHtcbiAgICAgICAgcmVxQW5pbUZyYW1lKGZyYW1lRnVuYyk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBpZiAoY2FsbGJhY2spIHsgY2FsbGJhY2soKTsgfVxuICAgICAgfVxuICAgIH07XG4gICAgcmVxQW5pbUZyYW1lKGZyYW1lRnVuYyk7XG4gIH1cblxufVxuXG5leHBvcnQgZnVuY3Rpb24gU0NST0xMX1NFUlZJQ0VfUFJPVklERVJfRkFDVE9SWShkb2M6IERvY3VtZW50LCBzY3JvbGxTZXJ2aWNlOiBOelNjcm9sbFNlcnZpY2UpOiBOelNjcm9sbFNlcnZpY2Uge1xuICByZXR1cm4gc2Nyb2xsU2VydmljZSB8fCBuZXcgTnpTY3JvbGxTZXJ2aWNlKGRvYyk7XG59XG5cbmV4cG9ydCBjb25zdCBTQ1JPTExfU0VSVklDRV9QUk9WSURFUjogUHJvdmlkZXIgPSB7XG4gIHByb3ZpZGUgICA6IE56U2Nyb2xsU2VydmljZSxcbiAgdXNlRmFjdG9yeTogU0NST0xMX1NFUlZJQ0VfUFJPVklERVJfRkFDVE9SWSxcbiAgZGVwcyAgICAgIDogWyBET0NVTUVOVCwgWyBuZXcgT3B0aW9uYWwoKSwgbmV3IFNraXBTZWxmKCksIE56U2Nyb2xsU2VydmljZSBdIF1cbn07XG4iXX0=