/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { DOCUMENT } from '@angular/common';
import { ChangeDetectionStrategy, Component, ElementRef, EventEmitter, Inject, Input, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { NzScrollService } from '../core/scroll/nz-scroll.service';
import { shallowEqual } from '../core/util/check';
import { toNumber } from '../core/util/convert';
import { throttleByAnimationFrameDecorator } from '../core/util/throttleByAnimationFrame';
var NzAffixComponent = /** @class */ (function () {
    // tslint:disable-next-line:no-any
    function NzAffixComponent(_el, scrollSrv, doc) {
        this.scrollSrv = scrollSrv;
        this.doc = doc;
        this.nzChange = new EventEmitter();
        this.events = ['resize', 'scroll', 'touchstart', 'touchmove', 'touchend', 'pageshow', 'load'];
        this._target = window;
        this.placeholderNode = _el.nativeElement;
    }
    Object.defineProperty(NzAffixComponent.prototype, "nzTarget", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.clearEventListeners();
            this._target = typeof value === 'string' ? this.doc.querySelector(value) : value || window;
            this.setTargetEventListeners();
            this.updatePosition((/** @type {?} */ ({})));
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzAffixComponent.prototype, "nzOffsetTop", {
        get: /**
         * @return {?}
         */
        function () {
            return this._offsetTop;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value === undefined || value === null) {
                return;
            }
            this._offsetTop = toNumber(value, null);
            this.updatePosition((/** @type {?} */ ({})));
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzAffixComponent.prototype, "nzOffsetBottom", {
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (typeof value === 'undefined') {
                return;
            }
            this._offsetBottom = toNumber(value, null);
            this.updatePosition((/** @type {?} */ ({})));
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzAffixComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.timeout = setTimeout((/**
         * @return {?}
         */
        function () {
            _this.setTargetEventListeners();
            _this.updatePosition((/** @type {?} */ ({})));
        }));
    };
    /**
     * @return {?}
     */
    NzAffixComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.clearEventListeners();
        clearTimeout(this.timeout);
        // tslint:disable-next-line:no-any
        ((/** @type {?} */ (this.updatePosition))).cancel();
    };
    /**
     * @param {?} element
     * @param {?} target
     * @return {?}
     */
    NzAffixComponent.prototype.getOffset = /**
     * @param {?} element
     * @param {?} target
     * @return {?}
     */
    function (element, target) {
        /** @type {?} */
        var elemRect = element.getBoundingClientRect();
        /** @type {?} */
        var targetRect = this.getTargetRect(target);
        /** @type {?} */
        var scrollTop = this.scrollSrv.getScroll(target, true);
        /** @type {?} */
        var scrollLeft = this.scrollSrv.getScroll(target, false);
        /** @type {?} */
        var docElem = this.doc.body;
        /** @type {?} */
        var clientTop = docElem.clientTop || 0;
        /** @type {?} */
        var clientLeft = docElem.clientLeft || 0;
        return {
            top: elemRect.top - targetRect.top + scrollTop - clientTop,
            left: elemRect.left - targetRect.left + scrollLeft - clientLeft,
            width: elemRect.width,
            height: elemRect.height
        };
    };
    /**
     * @private
     * @return {?}
     */
    NzAffixComponent.prototype.setTargetEventListeners = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        this.clearEventListeners();
        this.events.forEach((/**
         * @param {?} eventName
         * @return {?}
         */
        function (eventName) {
            _this._target.addEventListener(eventName, _this.updatePosition, false);
        }));
    };
    /**
     * @private
     * @return {?}
     */
    NzAffixComponent.prototype.clearEventListeners = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        this.events.forEach((/**
         * @param {?} eventName
         * @return {?}
         */
        function (eventName) {
            _this._target.removeEventListener(eventName, _this.updatePosition, false);
        }));
    };
    /**
     * @private
     * @param {?} target
     * @return {?}
     */
    NzAffixComponent.prototype.getTargetRect = /**
     * @private
     * @param {?} target
     * @return {?}
     */
    function (target) {
        return target !== window
            ? ((/** @type {?} */ (target))).getBoundingClientRect()
            : ((/** @type {?} */ ({ top: 0, left: 0, bottom: 0 })));
    };
    /**
     * @private
     * @param {?=} affixStyle
     * @return {?}
     */
    NzAffixComponent.prototype.genStyle = /**
     * @private
     * @param {?=} affixStyle
     * @return {?}
     */
    function (affixStyle) {
        if (!affixStyle) {
            return '';
        }
        return Object.keys(affixStyle)
            .map((/**
         * @param {?} key
         * @return {?}
         */
        function (key) {
            /** @type {?} */
            var val = affixStyle[key];
            return key + ":" + (typeof val === 'string' ? val : val + 'px');
        }))
            .join(';');
    };
    /**
     * @private
     * @param {?} e
     * @param {?=} affixStyle
     * @return {?}
     */
    NzAffixComponent.prototype.setAffixStyle = /**
     * @private
     * @param {?} e
     * @param {?=} affixStyle
     * @return {?}
     */
    function (e, affixStyle) {
        /** @type {?} */
        var originalAffixStyle = this.affixStyle;
        /** @type {?} */
        var isWindow = this._target === window;
        if (e.type === 'scroll' && originalAffixStyle && affixStyle && isWindow) {
            return;
        }
        if (shallowEqual(originalAffixStyle, affixStyle)) {
            return;
        }
        /** @type {?} */
        var fixed = !!affixStyle;
        /** @type {?} */
        var wrapEl = (/** @type {?} */ (this.fixedEl.nativeElement));
        wrapEl.style.cssText = this.genStyle(affixStyle);
        this.affixStyle = affixStyle;
        /** @type {?} */
        var cls = 'ant-affix';
        if (fixed) {
            wrapEl.classList.add(cls);
        }
        else {
            wrapEl.classList.remove(cls);
        }
        if ((affixStyle && !originalAffixStyle) || (!affixStyle && originalAffixStyle)) {
            this.nzChange.emit(fixed);
        }
    };
    /**
     * @private
     * @param {?=} placeholderStyle
     * @return {?}
     */
    NzAffixComponent.prototype.setPlaceholderStyle = /**
     * @private
     * @param {?=} placeholderStyle
     * @return {?}
     */
    function (placeholderStyle) {
        /** @type {?} */
        var originalPlaceholderStyle = this.placeholderStyle;
        if (shallowEqual(placeholderStyle, originalPlaceholderStyle)) {
            return;
        }
        this.placeholderNode.style.cssText = this.genStyle(placeholderStyle);
        this.placeholderStyle = placeholderStyle;
    };
    /**
     * @private
     * @param {?} e
     * @return {?}
     */
    NzAffixComponent.prototype.syncPlaceholderStyle = /**
     * @private
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (!this.affixStyle) {
            return;
        }
        this.placeholderNode.style.cssText = '';
        this.placeholderStyle = undefined;
        /** @type {?} */
        var styleObj = { width: this.placeholderNode.offsetWidth, height: this.fixedEl.nativeElement.offsetHeight };
        this.setAffixStyle(e, tslib_1.__assign({}, this.affixStyle, styleObj));
        this.setPlaceholderStyle(styleObj);
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzAffixComponent.prototype.updatePosition = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        /** @type {?} */
        var targetNode = this._target;
        // Backwards support
        /** @type {?} */
        var offsetTop = this.nzOffsetTop;
        /** @type {?} */
        var scrollTop = this.scrollSrv.getScroll(targetNode, true);
        /** @type {?} */
        var elemOffset = this.getOffset(this.placeholderNode, targetNode);
        /** @type {?} */
        var fixedNode = (/** @type {?} */ (this.fixedEl.nativeElement));
        /** @type {?} */
        var elemSize = {
            width: fixedNode.offsetWidth,
            height: fixedNode.offsetHeight
        };
        /** @type {?} */
        var offsetMode = {
            top: false,
            bottom: false
        };
        // Default to `offsetTop=0`.
        if (typeof offsetTop !== 'number' && typeof this._offsetBottom !== 'number') {
            offsetMode.top = true;
            offsetTop = 0;
        }
        else {
            offsetMode.top = typeof offsetTop === 'number';
            offsetMode.bottom = typeof this._offsetBottom === 'number';
        }
        /** @type {?} */
        var targetRect = this.getTargetRect(targetNode);
        /** @type {?} */
        var targetInnerHeight = ((/** @type {?} */ (targetNode))).innerHeight || ((/** @type {?} */ (targetNode))).clientHeight;
        if (scrollTop >= elemOffset.top - ((/** @type {?} */ (offsetTop))) && offsetMode.top) {
            /** @type {?} */
            var width = elemOffset.width;
            /** @type {?} */
            var top_1 = targetRect.top + ((/** @type {?} */ (offsetTop)));
            this.setAffixStyle(e, {
                position: 'fixed',
                top: top_1,
                left: targetRect.left + elemOffset.left,
                maxHeight: "calc(100vh - " + top_1 + "px)",
                width: width
            });
            this.setPlaceholderStyle({
                width: width,
                height: elemSize.height
            });
        }
        else if (scrollTop <= elemOffset.top + elemSize.height + ((/** @type {?} */ (this._offsetBottom))) - targetInnerHeight &&
            offsetMode.bottom) {
            /** @type {?} */
            var targetBottomOffet = targetNode === window ? 0 : window.innerHeight - targetRect.bottom;
            /** @type {?} */
            var width = elemOffset.width;
            this.setAffixStyle(e, {
                position: 'fixed',
                bottom: targetBottomOffet + ((/** @type {?} */ (this._offsetBottom))),
                left: targetRect.left + elemOffset.left,
                width: width
            });
            this.setPlaceholderStyle({
                width: width,
                height: elemOffset.height
            });
        }
        else {
            if (e.type === 'resize' &&
                this.affixStyle &&
                this.affixStyle.position === 'fixed' &&
                this.placeholderNode.offsetWidth) {
                this.setAffixStyle(e, tslib_1.__assign({}, this.affixStyle, { width: this.placeholderNode.offsetWidth }));
            }
            else {
                this.setAffixStyle(e);
            }
            this.setPlaceholderStyle();
        }
        if (e.type === 'resize') {
            this.syncPlaceholderStyle(e);
        }
    };
    NzAffixComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-affix',
                    template: "<div #fixedEl>\n  <ng-content></ng-content>\n</div>",
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    styles: ["\n      nz-affix {\n        display: block;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzAffixComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: NzScrollService },
        { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
    ]; };
    NzAffixComponent.propDecorators = {
        nzTarget: [{ type: Input }],
        nzOffsetTop: [{ type: Input }],
        nzOffsetBottom: [{ type: Input }],
        nzChange: [{ type: Output }],
        fixedEl: [{ type: ViewChild, args: ['fixedEl',] }]
    };
    tslib_1.__decorate([
        throttleByAnimationFrameDecorator(),
        tslib_1.__metadata("design:type", Function),
        tslib_1.__metadata("design:paramtypes", [Event]),
        tslib_1.__metadata("design:returntype", void 0)
    ], NzAffixComponent.prototype, "updatePosition", null);
    return NzAffixComponent;
}());
export { NzAffixComponent };
if (false) {
    /** @type {?} */
    NzAffixComponent.prototype.nzChange;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.timeout;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.events;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.fixedEl;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.placeholderNode;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.affixStyle;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.placeholderStyle;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype._target;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype._offsetTop;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype._offsetBottom;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.scrollSrv;
    /**
     * @type {?}
     * @private
     */
    NzAffixComponent.prototype.doc;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYWZmaXguY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImFmZml4L256LWFmZml4LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMzQyxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLE1BQU0sRUFDTixLQUFLLEVBR0wsTUFBTSxFQUNOLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLGtDQUFrQyxDQUFDO0FBRW5FLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUNsRCxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDaEQsT0FBTyxFQUFFLGlDQUFpQyxFQUFFLE1BQU0sdUNBQXVDLENBQUM7QUFFMUY7SUEwREUsa0NBQWtDO0lBQ2xDLDBCQUFZLEdBQWUsRUFBVSxTQUEwQixFQUE0QixHQUFRO1FBQTlELGNBQVMsR0FBVCxTQUFTLENBQWlCO1FBQTRCLFFBQUcsR0FBSCxHQUFHLENBQUs7UUFmaEYsYUFBUSxHQUFHLElBQUksWUFBWSxFQUFXLENBQUM7UUFHekMsV0FBTSxHQUFHLENBQUMsUUFBUSxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLFVBQVUsRUFBRSxVQUFVLEVBQUUsTUFBTSxDQUFDLENBQUM7UUFPbEcsWUFBTyxHQUFxQixNQUFNLENBQUM7UUFNekMsSUFBSSxDQUFDLGVBQWUsR0FBRyxHQUFHLENBQUMsYUFBYSxDQUFDO0lBQzNDLENBQUM7SUEvQ0Qsc0JBQ0ksc0NBQVE7Ozs7O1FBRFosVUFDYSxLQUFnQztZQUMzQyxJQUFJLENBQUMsbUJBQW1CLEVBQUUsQ0FBQztZQUMzQixJQUFJ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TyxJQUFJLENBQUMsVUFBVSxJQUFFLEtBQUssRUFBRSxJQUFJLENBQUMsZUFBZSxDQUFDLFdBQVcsSUFBRyxDQUFDO2FBQ3hGO2lCQUFNO2dCQUNMLElBQUksQ0FBQyxhQUFhLENBQUMsQ0FBQyxDQUFDLENBQUM7YUFDdkI7WUFDRCxJQUFJLENBQUMsbUJBQW1CLEVBQUUsQ0FBQztTQUM1QjtRQUVELElBQUksQ0FBQyxDQUFDLElBQUksS0FBSyxRQUFRLEVBQUU7WUFDdkIsSUFBSSxDQUFDLG9CQUFvQixDQUFDLENBQUMsQ0FBQyxDQUFDO1NBQzlCO0lBQ0gsQ0FBQzs7Z0JBalFGLFNBQVMsU0FBQztvQkFDVCxRQUFRLEVBQUUsVUFBVTtvQkFDcEIsK0RBQXdDO29CQUN4QyxlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtvQkFRL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7NkJBTm5DLDREQUlDO2lCQUdKOzs7O2dCQTdCQyxVQUFVO2dCQVdILGVBQWU7Z0RBaUU0QyxNQUFNLFNBQUMsUUFBUTs7OzJCQTdDaEYsS0FBSzs4QkFRTCxLQUFLO2lDQWFMLEtBQUs7MkJBU0wsTUFBTTswQkFJTixTQUFTLFNBQUMsU0FBUzs7SUF5SXBCO1FBREMsaUNBQWlDLEVBQUU7O2lEQUNsQixLQUFLOzswREF3RXRCO0lBQ0gsdUJBQUM7Q0FBQSxBQWxRRCxJQWtRQztTQXJQWSxnQkFBZ0I7OztJQStCM0Isb0NBQTBEOzs7OztJQUUxRCxtQ0FBd0I7Ozs7O0lBQ3hCLGtDQUEwRzs7Ozs7SUFDMUcsbUNBQWtEOzs7OztJQUVsRCwyQ0FBOEM7Ozs7O0lBRTlDLHNDQUFpRDs7Ozs7SUFDakQsNENBQXVEOzs7OztJQUN2RCxtQ0FBMkM7Ozs7O0lBQzNDLHNDQUFrQzs7Ozs7SUFDbEMseUNBQXFDOzs7OztJQUdSLHFDQUFrQzs7Ozs7SUFBRSwrQkFBa0MiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBET0NVTUVOVCB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge1xuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIEluamVjdCxcbiAgSW5wdXQsXG4gIE9uRGVzdHJveSxcbiAgT25Jbml0LFxuICBPdXRwdXQsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56U2Nyb2xsU2VydmljZSB9IGZyb20gJy4uL2NvcmUvc2Nyb2xsL256LXNjcm9sbC5zZXJ2aWNlJztcbmltcG9ydCB7IE5HU3R5bGVJbnRlcmZhY2UgfSBmcm9tICcuLi9jb3JlL3R5cGVzL25nLWNsYXNzJztcbmltcG9ydCB7IHNoYWxsb3dFcXVhbCB9IGZyb20gJy4uL2NvcmUvdXRpbC9jaGVjayc7XG5pbXBvcnQgeyB0b051bWJlciB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IHRocm90dGxlQnlBbmltYXRpb25GcmFtZURlY29yYXRvciB9IGZyb20gJy4uL2NvcmUvdXRpbC90aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWUnO1xuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1hZmZpeCcsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1hZmZpeC5jb21wb25lbnQuaHRtbCcsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei1hZmZpeCB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgfVxuICAgIGBcbiAgXSxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZVxufSlcbmV4cG9ydCBjbGFzcyBOekFmZml4Q29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkRlc3Ryb3kge1xuICBASW5wdXQoKVxuICBzZXQgbnpUYXJnZXQodmFsdWU6IHN0cmluZyB8IEVsZW1lbnQgfCBXaW5kb3cpIHtcbiAgICB0aGlzLmNsZWFyRXZlbnRMaXN0ZW5lcnMoKTtcbiAgICB0aGlzLl90YXJnZXQgPSB0eXBlb2YgdmFsdWUgPT09ICdzdHJpbmcnID8gdGhpcy5kb2MucXVlcnlTZWxlY3Rvcih2YWx1ZSkgOiB2YWx1ZSB8fCB3aW5kb3c7XG4gICAgdGhpcy5zZXRUYXJnZXRFdmVudExpc3RlbmVycygpO1xuICAgIHRoaXMudXBkYXRlUG9zaXRpb24oe30gYXMgRXZlbnQpO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56T2Zmc2V0VG9wKHZhbHVlOiBudW1iZXIgfCBudWxsKSB7XG4gICAgaWYgKHZhbHVlID09PSB1bmRlZmluZWQgfHwgdmFsdWUgPT09IG51bGwpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5fb2Zmc2V0VG9wID0gdG9OdW1iZXIodmFsdWUsIG51bGwpO1xuICAgIHRoaXMudXBkYXRlUG9zaXRpb24oe30gYXMgRXZlbnQpO1xuICB9XG5cbiAgZ2V0IG56T2Zmc2V0VG9wKCk6IG51bWJlciB8IG51bGwge1xuICAgIHJldHVybiB0aGlzLl9vZmZzZXRUb3A7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpPZmZzZXRCb3R0b20odmFsdWU6IG51bWJlcikge1xuICAgIGlmICh0eXBlb2YgdmFsdWUgPT09ICd1bmRlZmluZWQnKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuX29mZnNldEJvdHRvbSA9IHRvTnVtYmVyKHZhbHVlLCBudWxsKTtcbiAgICB0aGlzLnVwZGF0ZVBvc2l0aW9uKHt9IGFzIEV2ZW50KTtcbiAgfVxuXG4gIEBPdXRwdXQoKSByZWFkb25seSBuekNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Ym9vbGVhbj4oKTtcblxuICBwcml2YXRlIHRpbWVvdXQ6IG51bWJlcjtcbiAgcHJpdmF0ZSByZWFkb25seSBldmVudHMgPSBbJ3Jlc2l6ZScsICdzY3JvbGwnLCAndG91Y2hzdGFydCcsICd0b3VjaG1vdmUnLCAndG91Y2hlbmQnLCAncGFnZXNob3cnLCAnbG9hZCddO1xuICBAVmlld0NoaWxkKCdmaXhlZEVsJykgcHJpdmF0ZSBmaXhlZEVsOiBFbGVtZW50UmVmO1xuXG4gIHByaXZhdGUgcmVhZG9ubHkgcGxhY2Vob2xkZXJOb2RlOiBIVE1MRWxlbWVudDtcblxuICBwcml2YXRlIGFmZml4U3R5bGU6IE5HU3R5bGVJbnRlcmZhY2UgfCB1bmRlZmluZWQ7XG4gIHByaXZhdGUgcGxhY2Vob2xkZXJTdHlsZTogTkdTdHlsZUludGVyZmFjZSB8IHVuZGVmaW5lZDtcbiAgcHJpdmF0ZSBfdGFyZ2V0OiBFbGVtZW50IHwgV2luZG93ID0gd2luZG93O1xuICBwcml2YXRlIF9vZmZzZXRUb3A6IG51bWJlciB8IG51bGw7XG4gIHByaXZhdGUgX29mZnNldEJvdHRvbTogbnVtYmVyIHwgbnVsbDtcblxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIGNvbnN0cnVjdG9yKF9lbDogRWxlbWVudFJlZiwgcHJpdmF0ZSBzY3JvbGxTcnY6IE56U2Nyb2xsU2VydmljZSwgQEluamVjdChET0NVTUVOVCkgcHJpdmF0ZSBkb2M6IGFueSkge1xuICAgIHRoaXMucGxhY2Vob2xkZXJOb2RlID0gX2VsLm5hdGl2ZUVsZW1lbnQ7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnRpbWVvdXQgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMuc2V0VGFyZ2V0RXZlbnRMaXN0ZW5lcnMoKTtcbiAgICAgIHRoaXMudXBkYXRlUG9zaXRpb24oe30gYXMgRXZlbnQpO1xuICAgIH0pO1xuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5jbGVhckV2ZW50TGlzdGVuZXJzKCk7XG4gICAgY2xlYXJUaW1lb3V0KHRoaXMudGltZW91dCk7XG4gICAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICAgICh0aGlzLnVwZGF0ZVBvc2l0aW9uIGFzIGFueSkuY2FuY2VsKCk7XG4gIH1cblxuICBnZXRPZmZzZXQoXG4gICAgZWxlbWVudDogRWxlbWVudCxcbiAgICB0YXJnZXQ6IEVsZW1lbnQgfCBXaW5kb3cgfCB1bmRlZmluZWRcbiAgKToge1xuICAgIHRvcDogbnVtYmVyO1xuICAgIGxlZnQ6IG51bWJlcjtcbiAgICB3aWR0aDogbnVtYmVyO1xuICAgIGhlaWdodDogbnVtYmVyO1xuICB9IHtcbiAgICBjb25zdCBlbGVtUmVjdCA9IGVsZW1lbnQuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCk7XG4gICAgY29uc3QgdGFyZ2V0UmVjdCA9IHRoaXMuZ2V0VGFyZ2V0UmVjdCh0YXJnZXQpO1xuXG4gICAgY29uc3Qgc2Nyb2xsVG9wID0gdGhpcy5zY3JvbGxTcnYuZ2V0U2Nyb2xsKHRhcmdldCwgdHJ1ZSk7XG4gICAgY29uc3Qgc2Nyb2xsTGVmdCA9IHRoaXMuc2Nyb2xsU3J2LmdldFNjcm9sbCh0YXJnZXQsIGZhbHNlKTtcblxuICAgIGNvbnN0IGRvY0VsZW0gPSB0aGlzLmRvYy5ib2R5O1xuICAgIGNvbnN0IGNsaWVudFRvcCA9IGRvY0VsZW0uY2xpZW50VG9wIHx8IDA7XG4gICAgY29uc3QgY2xpZW50TGVmdCA9IGRvY0VsZW0uY2xpZW50TGVmdCB8fCAwO1xuXG4gICAgcmV0dXJuIHtcbiAgICAgIHRvcDogZWxlbVJlY3QudG9wIC0gdGFyZ2V0UmVjdC50b3AgKyBzY3JvbGxUb3AgLSBjbGllbnRUb3AsXG4gICAgICBsZWZ0OiBlbGVtUmVjdC5sZWZ0IC0gdGFyZ2V0UmVjdC5sZWZ0ICsgc2Nyb2xsTGVmdCAtIGNsaWVudExlZnQsXG4gICAgICB3aWR0aDogZWxlbVJlY3Qud2lkdGgsXG4gICAgICBoZWlnaHQ6IGVsZW1SZWN0LmhlaWdodFxuICAgIH07XG4gIH1cblxuICBwcml2YXRlIHNldFRhcmdldEV2ZW50TGlzdGVuZXJzKCk6IHZvaWQge1xuICAgIHRoaXMuY2xlYXJFdmVudExpc3RlbmVycygpO1xuICAgIHRoaXMuZXZlbnRzLmZvckVhY2goKGV2ZW50TmFtZTogc3RyaW5nKSA9PiB7XG4gICAgICB0aGlzLl90YXJnZXQuYWRkRXZlbnRMaXN0ZW5lcihldmVudE5hbWUsIHRoaXMudXBkYXRlUG9zaXRpb24sIGZhbHNlKTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgY2xlYXJFdmVudExpc3RlbmVycygpOiB2b2lkIHtcbiAgICB0aGlzLmV2ZW50cy5mb3JFYWNoKGV2ZW50TmFtZSA9PiB7XG4gICAgICB0aGlzLl90YXJnZXQucmVtb3ZlRXZlbnRMaXN0ZW5lcihldmVudE5hbWUsIHRoaXMudXBkYXRlUG9zaXRpb24sIGZhbHNlKTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgZ2V0VGFyZ2V0UmVjdCh0YXJnZXQ6IEVsZW1lbnQgfCBXaW5kb3cgfCB1bmRlZmluZWQpOiBDbGllbnRSZWN0IHtcbiAgICByZXR1cm4gdGFyZ2V0ICE9PSB3aW5kb3dcbiAgICAgID8gKHRhcmdldCBhcyBIVE1MRWxlbWVudCkuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KClcbiAgICAgIDogKHsgdG9wOiAwLCBsZWZ0OiAwLCBib3R0b206IDAgfSBhcyBDbGllbnRSZWN0KTtcbiAgfVxuXG4gIHByaXZhdGUgZ2VuU3R5bGUoYWZmaXhTdHlsZT86IE5HU3R5bGVJbnRlcmZhY2UpOiBzdHJpbmcge1xuICAgIGlmICghYWZmaXhTdHlsZSkge1xuICAgICAgcmV0dXJuICcnO1xuICAgIH1cbiAgICByZXR1cm4gT2JqZWN0LmtleXMoYWZmaXhTdHlsZSlcbiAgICAgIC5tYXAoa2V5ID0+IHtcbiAgICAgICAgY29uc3QgdmFsID0gYWZmaXhTdHlsZVtrZXldO1xuICAgICAgICByZXR1cm4gYCR7a2V5fToke3R5cGVvZiB2YWwgPT09ICdzdHJpbmcnID8gdmFsIDogdmFsICsgJ3B4J31gO1xuICAgICAgfSlcbiAgICAgIC5qb2luKCc7Jyk7XG4gIH1cblxuICBwcml2YXRlIHNldEFmZml4U3R5bGUoZTogRXZlbnQsIGFmZml4U3R5bGU/OiBOR1N0eWxlSW50ZXJmYWNlKTogdm9pZCB7XG4gICAgY29uc3Qgb3JpZ2luYWxBZmZpeFN0eWxlID0gdGhpcy5hZmZpeFN0eWxlO1xuICAgIGNvbnN0IGlzV2luZG93ID0gdGhpcy5fdGFyZ2V0ID09PSB3aW5kb3c7XG4gICAgaWYgKGUudHlwZSA9PT0gJ3Njcm9sbCcgJiYgb3JpZ2luYWxBZmZpeFN0eWxlICYmIGFmZml4U3R5bGUgJiYgaXNXaW5kb3cpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgaWYgKHNoYWxsb3dFcXVhbChvcmlnaW5hbEFmZml4U3R5bGUsIGFmZml4U3R5bGUpKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3QgZml4ZWQgPSAhIWFmZml4U3R5bGU7XG4gICAgY29uc3Qgd3JhcEVsID0gdGhpcy5maXhlZEVsLm5hdGl2ZUVsZW1lbnQgYXMgSFRNTEVsZW1lbnQ7XG4gICAgd3JhcEVsLnN0eWxlLmNzc1RleHQgPSB0aGlzLmdlblN0eWxlKGFmZml4U3R5bGUpO1xuICAgIHRoaXMuYWZmaXhTdHlsZSA9IGFmZml4U3R5bGU7XG4gICAgY29uc3QgY2xzID0gJ2FudC1hZmZpeCc7XG4gICAgaWYgKGZpeGVkKSB7XG4gICAgICB3cmFwRWwuY2xhc3NMaXN0LmFkZChjbHMpO1xuICAgIH0gZWxzZSB7XG4gICAgICB3cmFwRWwuY2xhc3NMaXN0LnJlbW92ZShjbHMpO1xuICAgIH1cblxuICAgIGlmICgoYWZmaXhTdHlsZSAmJiAhb3JpZ2luYWxBZmZpeFN0eWxlKSB8fCAoIWFmZml4U3R5bGUgJiYgb3JpZ2luYWxBZmZpeFN0eWxlKSkge1xuICAgICAgdGhpcy5uekNoYW5nZS5lbWl0KGZpeGVkKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIHNldFBsYWNlaG9sZGVyU3R5bGUocGxhY2Vob2xkZXJTdHlsZT86IE5HU3R5bGVJbnRlcmZhY2UpOiB2b2lkIHtcbiAgICBjb25zdCBvcmlnaW5hbFBsYWNlaG9sZGVyU3R5bGUgPSB0aGlzLnBsYWNlaG9sZGVyU3R5bGU7XG4gICAgaWYgKHNoYWxsb3dFcXVhbChwbGFjZWhvbGRlclN0eWxlLCBvcmlnaW5hbFBsYWNlaG9sZGVyU3R5bGUpKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMucGxhY2Vob2xkZXJOb2RlLnN0eWxlLmNzc1RleHQgPSB0aGlzLmdlblN0eWxlKHBsYWNlaG9sZGVyU3R5bGUpO1xuICAgIHRoaXMucGxhY2Vob2xkZXJTdHlsZSA9IHBsYWNlaG9sZGVyU3R5bGU7XG4gIH1cblxuICBwcml2YXRlIHN5bmNQbGFjZWhvbGRlclN0eWxlKGU6IEV2ZW50KTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLmFmZml4U3R5bGUpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5wbGFjZWhvbGRlck5vZGUuc3R5bGUuY3NzVGV4dCA9ICcnO1xuICAgIHRoaXMucGxhY2Vob2xkZXJTdHlsZSA9IHVuZGVmaW5lZDtcbiAgICBjb25zdCBzdHlsZU9iaiA9IHsgd2lkdGg6IHRoaXMucGxhY2Vob2xkZXJOb2RlLm9mZnNldFdpZHRoLCBoZWlnaHQ6IHRoaXMuZml4ZWRFbC5uYXRpdmVFbGVtZW50Lm9mZnNldEhlaWdodCB9O1xuICAgIHRoaXMuc2V0QWZmaXhTdHlsZShlLCB7XG4gICAgICAuLi50aGlzLmFmZml4U3R5bGUsXG4gICAgICAuLi5zdHlsZU9ialxuICAgIH0pO1xuICAgIHRoaXMuc2V0UGxhY2Vob2xkZXJTdHlsZShzdHlsZU9iaik7XG4gIH1cblxuICBAdGhyb3R0bGVCeUFuaW1hdGlvbkZyYW1lRGVjb3JhdG9yKClcbiAgdXBkYXRlUG9zaXRpb24oZTogRXZlbnQpOiB2b2lkIHtcbiAgICBjb25zdCB0YXJnZXROb2RlID0gdGhpcy5fdGFyZ2V0O1xuICAgIC8vIEJhY2t3YXJkcyBzdXBwb3J0XG4gICAgbGV0IG9mZnNldFRvcCA9IHRoaXMubnpPZmZzZXRUb3A7XG4gICAgY29uc3Qgc2Nyb2xsVG9wID0gdGhpcy5zY3JvbGxTcnYuZ2V0U2Nyb2xsKHRhcmdldE5vZGUsIHRydWUpO1xuICAgIGNvbnN0IGVsZW1PZmZzZXQgPSB0aGlzLmdldE9mZnNldCh0aGlzLnBsYWNlaG9sZGVyTm9kZSwgdGFyZ2V0Tm9kZSk7XG4gICAgY29uc3QgZml4ZWROb2RlID0gdGhpcy5maXhlZEVsLm5hdGl2ZUVsZW1lbnQgYXMgSFRNTEVsZW1lbnQ7XG4gICAgY29uc3QgZWxlbVNpemUgPSB7XG4gICAgICB3aWR0aDogZml4ZWROb2RlLm9mZnNldFdpZHRoLFxuICAgICAgaGVpZ2h0OiBmaXhlZE5vZGUub2Zmc2V0SGVpZ2h0XG4gICAgfTtcbiAgICBjb25zdCBvZmZzZXRNb2RlID0ge1xuICAgICAgdG9wOiBmYWxzZSxcbiAgICAgIGJvdHRvbTogZmFsc2VcbiAgICB9O1xuICAgIC8vIERlZmF1bHQgdG8gYG9mZnNldFRvcD0wYC5cbiAgICBpZiAodHlwZW9mIG9mZnNldFRvcCAhPT0gJ251bWJlcicgJiYgdHlwZW9mIHRoaXMuX29mZnNldEJvdHRvbSAhPT0gJ251bWJlcicpIHtcbiAgICAgIG9mZnNldE1vZGUudG9wID0gdHJ1ZTtcbiAgICAgIG9mZnNldFRvcCA9IDA7XG4gICAgfSBlbHNlIHtcbiAgICAgIG9mZnNldE1vZGUudG9wID0gdHlwZW9mIG9mZnNldFRvcCA9PT0gJ251bWJlcic7XG4gICAgICBvZmZzZXRNb2RlLmJvdHRvbSA9IHR5cGVvZiB0aGlzLl9vZmZzZXRCb3R0b20gPT09ICdudW1iZXInO1xuICAgIH1cbiAgICBjb25zdCB0YXJnZXRSZWN0ID0gdGhpcy5nZXRUYXJnZXRSZWN0KHRhcmdldE5vZGUpO1xuICAgIGNvbnN0IHRhcmdldElubmVySGVpZ2h0ID0gKHRhcmdldE5vZGUgYXMgV2luZG93KS5pbm5lckhlaWdodCB8fCAodGFyZ2V0Tm9kZSBhcyBIVE1MRWxlbWVudCkuY2xpZW50SGVpZ2h0O1xuICAgIGlmIChzY3JvbGxUb3AgPj0gZWxlbU9mZnNldC50b3AgLSAob2Zmc2V0VG9wIGFzIG51bWJlcikgJiYgb2Zmc2V0TW9kZS50b3ApIHtcbiAgICAgIGNvbnN0IHdpZHRoID0gZWxlbU9mZnNldC53aWR0aDtcbiAgICAgIGNvbnN0IHRvcCA9IHRhcmdldFJlY3QudG9wICsgKG9mZnNldFRvcCBhcyBudW1iZXIpO1xuICAgICAgdGhpcy5zZXRBZmZpeFN0eWxlKGUsIHtcbiAgICAgICAgcG9zaXRpb246ICdmaXhlZCcsXG4gICAgICAgIHRvcCxcbiAgICAgICAgbGVmdDogdGFyZ2V0UmVjdC5sZWZ0ICsgZWxlbU9mZnNldC5sZWZ0LFxuICAgICAgICBtYXhIZWlnaHQ6IGBjYWxjKDEwMHZoIC0gJHt0b3B9cHgpYCxcbiAgICAgICAgd2lkdGhcbiAgICAgIH0pO1xuICAgICAgdGhpcy5zZXRQbGFjZWhvbGRlclN0eWxlKHtcbiAgICAgICAgd2lkdGgsXG4gICAgICAgIGhlaWdodDogZWxlbVNpemUuaGVpZ2h0XG4gICAgICB9KTtcbiAgICB9IGVsc2UgaWYgKFxuICAgICAgc2Nyb2xsVG9wIDw9IGVsZW1PZmZzZXQudG9wICsgZWxlbVNpemUuaGVpZ2h0ICsgKHRoaXMuX29mZnNldEJvdHRvbSBhcyBudW1iZXIpIC0gdGFyZ2V0SW5uZXJIZWlnaHQgJiZcbiAgICAgIG9mZnNldE1vZGUuYm90dG9tXG4gICAgKSB7XG4gICAgICBjb25zdCB0YXJnZXRCb3R0b21PZmZldCA9IHRhcmdldE5vZGUgPT09IHdpbmRvdyA/IDAgOiB3aW5kb3cuaW5uZXJIZWlnaHQgLSB0YXJnZXRSZWN0LmJvdHRvbTtcbiAgICAgIGNvbnN0IHdpZHRoID0gZWxlbU9mZnNldC53aWR0aDtcbiAgICAgIHRoaXMuc2V0QWZmaXhTdHlsZShlLCB7XG4gICAgICAgIHBvc2l0aW9uOiAnZml4ZWQnLFxuICAgICAgICBib3R0b206IHRhcmdldEJvdHRvbU9mZmV0ICsgKHRoaXMuX29mZnNldEJvdHRvbSBhcyBudW1iZXIpLFxuICAgICAgICBsZWZ0OiB0YXJnZXRSZWN0LmxlZnQgKyBlbGVtT2Zmc2V0LmxlZnQsXG4gICAgICAgIHdpZHRoXG4gICAgICB9KTtcbiAgICAgIHRoaXMuc2V0UGxhY2Vob2xkZXJTdHlsZSh7XG4gICAgICAgIHdpZHRoLFxuICAgICAgICBoZWlnaHQ6IGVsZW1PZmZzZXQuaGVpZ2h0XG4gICAgICB9KTtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKFxuICAgICAgICBlLnR5cGUgPT09ICdyZXNpemUnICYmXG4gICAgICAgIHRoaXMuYWZmaXhTdHlsZSAmJlxuICAgICAgICB0aGlzLmFmZml4U3R5bGUucG9zaXRpb24gPT09ICdmaXhlZCcgJiZcbiAgICAgICAgdGhpcy5wbGFjZWhvbGRlck5vZGUub2Zmc2V0V2lkdGhcbiAgICAgICkge1xuICAgICAgICB0aGlzLnNldEFmZml4U3R5bGUoZSwgeyAuLi50aGlzLmFmZml4U3R5bGUsIHdpZHRoOiB0aGlzLnBsYWNlaG9sZGVyTm9kZS5vZmZzZXRXaWR0aCB9KTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc2V0QWZmaXhTdHlsZShlKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuc2V0UGxhY2Vob2xkZXJTdHlsZSgpO1xuICAgIH1cblxuICAgIGlmIChlLnR5cGUgPT09ICdyZXNpemUnKSB7XG4gICAgICB0aGlzLnN5bmNQbGFjZWhvbGRlclN0eWxlKGUpO1xuICAgIH1cbiAgfVxufVxuIl19