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
export class NzAffixComponent {
    // tslint:disable-next-line:no-any
    /**
     * @param {?} _el
     * @param {?} scrollSrv
     * @param {?} doc
     */
    constructor(_el, scrollSrv, doc) {
        this.scrollSrv = scrollSrv;
        this.doc = doc;
        this.nzChange = new EventEmitter();
        this.events = ['resize', 'scroll', 'touchstart', 'touchmove', 'touchend', 'pageshow', 'load'];
        this._target = window;
        this.placeholderNode = _el.nativeElement;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzTarget(value) {
        this.clearEventListeners();
        this._target = typeof value === 'string' ? this.doc.querySelector(value) : value || window;
        this.setTargetEventListeners();
        this.updatePosition((/** @type {?} */ ({})));
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzOffsetTop(value) {
        if (value === undefined || value === null) {
            return;
        }
        this._offsetTop = toNumber(value, null);
        this.updatePosition((/** @type {?} */ ({})));
    }
    /**
     * @return {?}
     */
    get nzOffsetTop() {
        return this._offsetTop;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzOffsetBottom(value) {
        if (typeof value === 'undefined') {
            return;
        }
        this._offsetBottom = toNumber(value, null);
        this.updatePosition((/** @type {?} */ ({})));
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.timeout = setTimeout((/**
         * @return {?}
         */
        () => {
            this.setTargetEventListeners();
            this.updatePosition((/** @type {?} */ ({})));
        }));
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.clearEventListeners();
        clearTimeout(this.timeout);
        // tslint:disable-next-line:no-any
        ((/** @type {?} */ (this.updatePosition))).cancel();
    }
    /**
     * @param {?} element
     * @param {?} target
     * @return {?}
     */
    getOffset(element, target) {
        /** @type {?} */
        const elemRect = element.getBoundingClientRect();
        /** @type {?} */
        const targetRect = this.getTargetRect(target);
        /** @type {?} */
        const scrollTop = this.scrollSrv.getScroll(target, true);
        /** @type {?} */
        const scrollLeft = this.scrollSrv.getScroll(target, false);
        /** @type {?} */
        const docElem = this.doc.body;
        /** @type {?} */
        const clientTop = docElem.clientTop || 0;
        /** @type {?} */
        const clientLeft = docElem.clientLeft || 0;
        return {
            top: elemRect.top - targetRect.top + scrollTop - clientTop,
            left: elemRect.left - targetRect.left + scrollLeft - clientLeft,
            width: elemRect.width,
            height: elemRect.height
        };
    }
    /**
     * @private
     * @return {?}
     */
    setTargetEventListeners() {
        this.clearEventListeners();
        this.events.forEach((/**
         * @param {?} eventName
         * @return {?}
         */
        (eventName) => {
            this._target.addEventListener(eventName, this.updatePosition, false);
        }));
    }
    /**
     * @private
     * @return {?}
     */
    clearEventListeners() {
        this.events.forEach((/**
         * @param {?} eventName
         * @return {?}
         */
        eventName => {
            this._target.removeEventListener(eventName, this.updatePosition, false);
        }));
    }
    /**
     * @private
     * @param {?} target
     * @return {?}
     */
    getTargetRect(target) {
        return target !== window
            ? ((/** @type {?} */ (target))).getBoundingClientRect()
            : ((/** @type {?} */ ({ top: 0, left: 0, bottom: 0 })));
    }
    /**
     * @private
     * @param {?=} affixStyle
     * @return {?}
     */
    genStyle(affixStyle) {
        if (!affixStyle) {
            return '';
        }
        return Object.keys(affixStyle)
            .map((/**
         * @param {?} key
         * @return {?}
         */
        key => {
            /** @type {?} */
            const val = affixStyle[key];
            return `${key}:${typeof val === 'string' ? val : val + 'px'}`;
        }))
            .join(';');
    }
    /**
     * @private
     * @param {?} e
     * @param {?=} affixStyle
     * @return {?}
     */
    setAffixStyle(e, affixStyle) {
        /** @type {?} */
        const originalAffixStyle = this.affixStyle;
        /** @type {?} */
        const isWindow = this._target === window;
        if (e.type === 'scroll' && originalAffixStyle && affixStyle && isWindow) {
            return;
        }
        if (shallowEqual(originalAffixStyle, affixStyle)) {
            return;
        }
        /** @type {?} */
        const fixed = !!affixStyle;
        /** @type {?} */
        const wrapEl = (/** @type {?} */ (this.fixedEl.nativeElement));
        wrapEl.style.cssText = this.genStyle(affixStyle);
        this.affixStyle = affixStyle;
        /** @type {?} */
        const cls = 'ant-affix';
        if (fixed) {
            wrapEl.classList.add(cls);
        }
        else {
            wrapEl.classList.remove(cls);
        }
        if ((affixStyle && !originalAffixStyle) || (!affixStyle && originalAffixStyle)) {
            this.nzChange.emit(fixed);
        }
    }
    /**
     * @private
     * @param {?=} placeholderStyle
     * @return {?}
     */
    setPlaceholderStyle(placeholderStyle) {
        /** @type {?} */
        const originalPlaceholderStyle = this.placeholderStyle;
        if (shallowEqual(placeholderStyle, originalPlaceholderStyle)) {
            return;
        }
        this.placeholderNode.style.cssText = this.genStyle(placeholderStyle);
        this.placeholderStyle = placeholderStyle;
    }
    /**
     * @private
     * @param {?} e
     * @return {?}
     */
    syncPlaceholderStyle(e) {
        if (!this.affixStyle) {
            return;
        }
        this.placeholderNode.style.cssText = '';
        this.placeholderStyle = undefined;
        /** @type {?} */
        const styleObj = { width: this.placeholderNode.offsetWidth, height: this.fixedEl.nativeElement.offsetHeight };
        this.setAffixStyle(e, Object.assign({}, this.affixStyle, styleObj));
        this.setPlaceholderStyle(styleObj);
    }
    /**
     * @param {?} e
     * @return {?}
     */
    updatePosition(e) {
        /** @type {?} */
        const targetNode = this._target;
        // Backwards support
        /** @type {?} */
        let offsetTop = this.nzOffsetTop;
        /** @type {?} */
        const scrollTop = this.scrollSrv.getScroll(targetNode, true);
        /** @type {?} */
        const elemOffset = this.getOffset(this.placeholderNode, targetNode);
        /** @type {?} */
        const fixedNode = (/** @type {?} */ (this.fixedEl.nativeElement));
        /** @type {?} */
        const elemSize = {
            width: fixedNode.offsetWidth,
            height: fixedNode.offsetHeight
        };
        /** @type {?} */
        const offsetMode = {
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
        const targetRect = this.getTargetRect(targetNode);
        /** @type {?} */
        const targetInnerHeight = ((/** @type {?} */ (targetNode))).innerHeight || ((/** @type {?} */ (targetNode))).clientHeight;
        if (scrollTop >= elemOffset.top - ((/** @type {?} */ (offsetTop))) && offsetMode.top) {
            /** @type {?} */
            const width = elemOffset.width;
            /** @type {?} */
            const top = targetRect.top + ((/** @type {?} */ (offsetTop)));
            this.setAffixStyle(e, {
                position: 'fixed',
                top,
                left: targetRect.left + elemOffset.left,
                maxHeight: `calc(100vh - ${top}px)`,
                width
            });
            this.setPlaceholderStyle({
                width,
                height: elemSize.height
            });
        }
        else if (scrollTop <= elemOffset.top + elemSize.height + ((/** @type {?} */ (this._offsetBottom))) - targetInnerHeight &&
            offsetMode.bottom) {
            /** @type {?} */
            const targetBottomOffet = targetNode === window ? 0 : window.innerHeight - targetRect.bottom;
            /** @type {?} */
            const width = elemOffset.width;
            this.setAffixStyle(e, {
                position: 'fixed',
                bottom: targetBottomOffet + ((/** @type {?} */ (this._offsetBottom))),
                left: targetRect.left + elemOffset.left,
                width
            });
            this.setPlaceholderStyle({
                width,
                height: elemOffset.height
            });
        }
        else {
            if (e.type === 'resize' &&
                this.affixStyle &&
                this.affixStyle.position === 'fixed' &&
                this.placeholderNode.offsetWidth) {
                this.setAffixStyle(e, Object.assign({}, this.affixStyle, { width: this.placeholderNode.offsetWidth }));
            }
            else {
                this.setAffixStyle(e);
            }
            this.setPlaceholderStyle();
        }
        if (e.type === 'resize') {
            this.syncPlaceholderStyle(e);
        }
    }
}
NzAffixComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-affix',
                template: "<div #fixedEl>\n  <ng-content></ng-content>\n</div>",
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                styles: [`
      nz-affix {
        display: block;
      }
    `]
            }] }
];
/** @nocollapse */
NzAffixComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: NzScrollService },
    { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] }
];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYWZmaXguY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImFmZml4L256LWFmZml4LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMzQyxPQUFPLEVBQ0wsdUJBQXVCLEVBQ3ZCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLE1BQU0sRUFDTixLQUFLLEVBR0wsTUFBTSxFQUNOLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLGtDQUFrQyxDQUFDO0FBRW5FLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUNsRCxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDaEQsT0FBTyxFQUFFLGlDQUFpQyxFQUFFLE1BQU0sdUNBQXVDLENBQUM7QUFlMUYsTUFBTSxPQUFPLGdCQUFnQjs7Ozs7OztJQThDM0IsWUFBWSxHQUFlLEVBQVUsU0FBMEIsRUFBNEIsR0FBUTtRQUE5RCxjQUFTLEdBQVQsU0FBUyxDQUFpQjtRQUE0QixRQUFHLEdBQUgsR0FBRyxDQUFLO1FBZmhGLGFBQVEsR0FBRyxJQUFJLFlBQVksRUFBVyxDQUFDO1FBR3pDLFdBQU0sR0FBRyxDQUFDLFFBQVEsRUFBRSxRQUFRLEVBQUUsWUFBWSxFQUFFLFdBQVcsRUFBRSxVQUFVLEVBQUUsVUFBVSxFQUFFLE1BQU0sQ0FBQyxDQUFDO1FBT2xHLFlBQU8sR0FBcUIsTUFBTSxDQUFDO1FBTXpDLElBQUksQ0FBQyxlQUFlLEdBQUcsR0FBRyxDQUFDLGFBQWEsQ0FBQztJQUMzQyxDQUFDOzs7OztJQS9DRCxJQUNJLFFBQVEsQ0FBQyxLQUFnQztRQUMzQyxJQUFJLENBQUMsbUJBQW1CLEVBQUUsQ0FBQztRQUMzQixJ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QzVCO1FBRUQsSUFBSSxDQUFDLENBQUMsSUFBSSxLQUFLLFFBQVEsRUFBRTtZQUN2QixJQUFJLENBQUMsb0JBQW9CLENBQUMsQ0FBQyxDQUFDLENBQUM7U0FDOUI7SUFDSCxDQUFDOzs7WUFqUUYsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxVQUFVO2dCQUNwQiwrREFBd0M7Z0JBQ3hDLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO2dCQVEvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTt5QkFObkM7Ozs7S0FJQzthQUdKOzs7O1lBN0JDLFVBQVU7WUFXSCxlQUFlOzRDQWlFNEMsTUFBTSxTQUFDLFFBQVE7Ozt1QkE3Q2hGLEtBQUs7MEJBUUwsS0FBSzs2QkFhTCxLQUFLO3VCQVNMLE1BQU07c0JBSU4sU0FBUyxTQUFDLFNBQVM7O0FBeUlwQjtJQURDLGlDQUFpQyxFQUFFOzs2Q0FDbEIsS0FBSzs7c0RBd0V0Qjs7O0lBck5ELG9DQUEwRDs7Ozs7SUFFMUQsbUNBQXdCOzs7OztJQUN4QixrQ0FBMEc7Ozs7O0lBQzFHLG1DQUFrRDs7Ozs7SUFFbEQsMkNBQThDOzs7OztJQUU5QyxzQ0FBaUQ7Ozs7O0lBQ2pELDRDQUF1RDs7Ozs7SUFDdkQsbUNBQTJDOzs7OztJQUMzQyxzQ0FBa0M7Ozs7O0lBQ2xDLHlDQUFxQzs7Ozs7SUFHUixxQ0FBa0M7Ozs7O0lBQUUsK0JBQWtDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgRE9DVU1FTlQgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENvbXBvbmVudCxcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbmplY3QsXG4gIElucHV0LFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOelNjcm9sbFNlcnZpY2UgfSBmcm9tICcuLi9jb3JlL3Njcm9sbC9uei1zY3JvbGwuc2VydmljZSc7XG5pbXBvcnQgeyBOR1N0eWxlSW50ZXJmYWNlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9uZy1jbGFzcyc7XG5pbXBvcnQgeyBzaGFsbG93RXF1YWwgfSBmcm9tICcuLi9jb3JlL3V0aWwvY2hlY2snO1xuaW1wb3J0IHsgdG9OdW1iZXIgfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyB0aHJvdHRsZUJ5QW5pbWF0aW9uRnJhbWVEZWNvcmF0b3IgfSBmcm9tICcuLi9jb3JlL3V0aWwvdGhyb3R0bGVCeUFuaW1hdGlvbkZyYW1lJztcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotYWZmaXgnLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotYWZmaXguY29tcG9uZW50Lmh0bWwnLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgc3R5bGVzOiBbXG4gICAgYFxuICAgICAgbnotYWZmaXgge1xuICAgICAgICBkaXNwbGF5OiBibG9jaztcbiAgICAgIH1cbiAgICBgXG4gIF0sXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmVcbn0pXG5leHBvcnQgY2xhc3MgTnpBZmZpeENvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25EZXN0cm95IHtcbiAgQElucHV0KClcbiAgc2V0IG56VGFyZ2V0KHZhbHVlOiBzdHJpbmcgfCBFbGVtZW50IHwgV2luZG93KSB7XG4gICAgdGhpcy5jbGVhckV2ZW50TGlzdGVuZXJzKCk7XG4gICAgdGhpcy5fdGFyZ2V0ID0gdHlwZW9mIHZhbHVlID09PSAnc3RyaW5nJyA/IHRoaXMuZG9jLnF1ZXJ5U2VsZWN0b3IodmFsdWUpIDogdmFsdWUgfHwgd2luZG93O1xuICAgIHRoaXMuc2V0VGFyZ2V0RXZlbnRMaXN0ZW5lcnMoKTtcbiAgICB0aGlzLnVwZGF0ZVBvc2l0aW9uKHt9IGFzIEV2ZW50KTtcbiAgfVxuXG4gIEBJbnB1dCgpXG4gIHNldCBuek9mZnNldFRvcCh2YWx1ZTogbnVtYmVyIHwgbnVsbCkge1xuICAgIGlmICh2YWx1ZSA9PT0gdW5kZWZpbmVkIHx8IHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMuX29mZnNldFRvcCA9IHRvTnVtYmVyKHZhbHVlLCBudWxsKTtcbiAgICB0aGlzLnVwZGF0ZVBvc2l0aW9uKHt9IGFzIEV2ZW50KTtcbiAgfVxuXG4gIGdldCBuek9mZnNldFRvcCgpOiBudW1iZXIgfCBudWxsIHtcbiAgICByZXR1cm4gdGhpcy5fb2Zmc2V0VG9wO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56T2Zmc2V0Qm90dG9tKHZhbHVlOiBudW1iZXIpIHtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSAndW5kZWZpbmVkJykge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLl9vZmZzZXRCb3R0b20gPSB0b051bWJlcih2YWx1ZSwgbnVsbCk7XG4gICAgdGhpcy51cGRhdGVQb3NpdGlvbih7fSBhcyBFdmVudCk7XG4gIH1cblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpDaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPGJvb2xlYW4+KCk7XG5cbiAgcHJpdmF0ZSB0aW1lb3V0OiBudW1iZXI7XG4gIHByaXZhdGUgcmVhZG9ubHkgZXZlbnRzID0gWydyZXNpemUnLCAnc2Nyb2xsJywgJ3RvdWNoc3RhcnQnLCAndG91Y2htb3ZlJywgJ3RvdWNoZW5kJywgJ3BhZ2VzaG93JywgJ2xvYWQnXTtcbiAgQFZpZXdDaGlsZCgnZml4ZWRFbCcpIHByaXZhdGUgZml4ZWRFbDogRWxlbWVudFJlZjtcblxuICBwcml2YXRlIHJlYWRvbmx5IHBsYWNlaG9sZGVyTm9kZTogSFRNTEVsZW1lbnQ7XG5cbiAgcHJpdmF0ZSBhZmZpeFN0eWxlOiBOR1N0eWxlSW50ZXJmYWNlIHwgdW5kZWZpbmVkO1xuICBwcml2YXRlIHBsYWNlaG9sZGVyU3R5bGU6IE5HU3R5bGVJbnRlcmZhY2UgfCB1bmRlZmluZWQ7XG4gIHByaXZhdGUgX3RhcmdldDogRWxlbWVudCB8IFdpbmRvdyA9IHdpbmRvdztcbiAgcHJpdmF0ZSBfb2Zmc2V0VG9wOiBudW1iZXIgfCBudWxsO1xuICBwcml2YXRlIF9vZmZzZXRCb3R0b206IG51bWJlciB8IG51bGw7XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBjb25zdHJ1Y3RvcihfZWw6IEVsZW1lbnRSZWYsIHByaXZhdGUgc2Nyb2xsU3J2OiBOelNjcm9sbFNlcnZpY2UsIEBJbmplY3QoRE9DVU1FTlQpIHByaXZhdGUgZG9jOiBhbnkpIHtcbiAgICB0aGlzLnBsYWNlaG9sZGVyTm9kZSA9IF9lbC5uYXRpdmVFbGVtZW50O1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy50aW1lb3V0ID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICB0aGlzLnNldFRhcmdldEV2ZW50TGlzdGVuZXJzKCk7XG4gICAgICB0aGlzLnVwZGF0ZVBvc2l0aW9uKHt9IGFzIEV2ZW50KTtcbiAgICB9KTtcbiAgfVxuXG4gIG5nT25EZXN0cm95KCk6IHZvaWQge1xuICAgIHRoaXMuY2xlYXJFdmVudExpc3RlbmVycygpO1xuICAgIGNsZWFyVGltZW91dCh0aGlzLnRpbWVvdXQpO1xuICAgIC8vIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnlcbiAgICAodGhpcy51cGRhdGVQb3NpdGlvbiBhcyBhbnkpLmNhbmNlbCgpO1xuICB9XG5cbiAgZ2V0T2Zmc2V0KFxuICAgIGVsZW1lbnQ6IEVsZW1lbnQsXG4gICAgdGFyZ2V0OiBFbGVtZW50IHwgV2luZG93IHwgdW5kZWZpbmVkXG4gICk6IHtcbiAgICB0b3A6IG51bWJlcjtcbiAgICBsZWZ0OiBudW1iZXI7XG4gICAgd2lkdGg6IG51bWJlcjtcbiAgICBoZWlnaHQ6IG51bWJlcjtcbiAgfSB7XG4gICAgY29uc3QgZWxlbVJlY3QgPSBlbGVtZW50LmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpO1xuICAgIGNvbnN0IHRhcmdldFJlY3QgPSB0aGlzLmdldFRhcmdldFJlY3QodGFyZ2V0KTtcblxuICAgIGNvbnN0IHNjcm9sbFRvcCA9IHRoaXMuc2Nyb2xsU3J2LmdldFNjcm9sbCh0YXJnZXQsIHRydWUpO1xuICAgIGNvbnN0IHNjcm9sbExlZnQgPSB0aGlzLnNjcm9sbFNydi5nZXRTY3JvbGwodGFyZ2V0LCBmYWxzZSk7XG5cbiAgICBjb25zdCBkb2NFbGVtID0gdGhpcy5kb2MuYm9keTtcbiAgICBjb25zdCBjbGllbnRUb3AgPSBkb2NFbGVtLmNsaWVudFRvcCB8fCAwO1xuICAgIGNvbnN0IGNsaWVudExlZnQgPSBkb2NFbGVtLmNsaWVudExlZnQgfHwgMDtcblxuICAgIHJldHVybiB7XG4gICAgICB0b3A6IGVsZW1SZWN0LnRvcCAtIHRhcmdldFJlY3QudG9wICsgc2Nyb2xsVG9wIC0gY2xpZW50VG9wLFxuICAgICAgbGVmdDogZWxlbVJlY3QubGVmdCAtIHRhcmdldFJlY3QubGVmdCArIHNjcm9sbExlZnQgLSBjbGllbnRMZWZ0LFxuICAgICAgd2lkdGg6IGVsZW1SZWN0LndpZHRoLFxuICAgICAgaGVpZ2h0OiBlbGVtUmVjdC5oZWlnaHRcbiAgICB9O1xuICB9XG5cbiAgcHJpdmF0ZSBzZXRUYXJnZXRFdmVudExpc3RlbmVycygpOiB2b2lkIHtcbiAgICB0aGlzLmNsZWFyRXZlbnRMaXN0ZW5lcnMoKTtcbiAgICB0aGlzLmV2ZW50cy5mb3JFYWNoKChldmVudE5hbWU6IHN0cmluZykgPT4ge1xuICAgICAgdGhpcy5fdGFyZ2V0LmFkZEV2ZW50TGlzdGVuZXIoZXZlbnROYW1lLCB0aGlzLnVwZGF0ZVBvc2l0aW9uLCBmYWxzZSk7XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIGNsZWFyRXZlbnRMaXN0ZW5lcnMoKTogdm9pZCB7XG4gICAgdGhpcy5ldmVudHMuZm9yRWFjaChldmVudE5hbWUgPT4ge1xuICAgICAgdGhpcy5fdGFyZ2V0LnJlbW92ZUV2ZW50TGlzdGVuZXIoZXZlbnROYW1lLCB0aGlzLnVwZGF0ZVBvc2l0aW9uLCBmYWxzZSk7XG4gICAgfSk7XG4gIH1cblxuICBwcml2YXRlIGdldFRhcmdldFJlY3QodGFyZ2V0OiBFbGVtZW50IHwgV2luZG93IHwgdW5kZWZpbmVkKTogQ2xpZW50UmVjdCB7XG4gICAgcmV0dXJuIHRhcmdldCAhPT0gd2luZG93XG4gICAgICA/ICh0YXJnZXQgYXMgSFRNTEVsZW1lbnQpLmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpXG4gICAgICA6ICh7IHRvcDogMCwgbGVmdDogMCwgYm90dG9tOiAwIH0gYXMgQ2xpZW50UmVjdCk7XG4gIH1cblxuICBwcml2YXRlIGdlblN0eWxlKGFmZml4U3R5bGU/OiBOR1N0eWxlSW50ZXJmYWNlKTogc3RyaW5nIHtcbiAgICBpZiAoIWFmZml4U3R5bGUpIHtcbiAgICAgIHJldHVybiAnJztcbiAgICB9XG4gICAgcmV0dXJuIE9iamVjdC5rZXlzKGFmZml4U3R5bGUpXG4gICAgICAubWFwKGtleSA9PiB7XG4gICAgICAgIGNvbnN0IHZhbCA9IGFmZml4U3R5bGVba2V5XTtcbiAgICAgICAgcmV0dXJuIGAke2tleX06JHt0eXBlb2YgdmFsID09PSAnc3RyaW5nJyA/IHZhbCA6IHZhbCArICdweCd9YDtcbiAgICAgIH0pXG4gICAgICAuam9pbignOycpO1xuICB9XG5cbiAgcHJpdmF0ZSBzZXRBZmZpeFN0eWxlKGU6IEV2ZW50LCBhZmZpeFN0eWxlPzogTkdTdHlsZUludGVyZmFjZSk6IHZvaWQge1xuICAgIGNvbnN0IG9yaWdpbmFsQWZmaXhTdHlsZSA9IHRoaXMuYWZmaXhTdHlsZTtcbiAgICBjb25zdCBpc1dpbmRvdyA9IHRoaXMuX3RhcmdldCA9PT0gd2luZG93O1xuICAgIGlmIChlLnR5cGUgPT09ICdzY3JvbGwnICYmIG9yaWdpbmFsQWZmaXhTdHlsZSAmJiBhZmZpeFN0eWxlICYmIGlzV2luZG93KSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmIChzaGFsbG93RXF1YWwob3JpZ2luYWxBZmZpeFN0eWxlLCBhZmZpeFN0eWxlKSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IGZpeGVkID0gISFhZmZpeFN0eWxlO1xuICAgIGNvbnN0IHdyYXBFbCA9IHRoaXMuZml4ZWRFbC5uYXRpdmVFbGVtZW50IGFzIEhUTUxFbGVtZW50O1xuICAgIHdyYXBFbC5zdHlsZS5jc3NUZXh0ID0gdGhpcy5nZW5TdHlsZShhZmZpeFN0eWxlKTtcbiAgICB0aGlzLmFmZml4U3R5bGUgPSBhZmZpeFN0eWxlO1xuICAgIGNvbnN0IGNscyA9ICdhbnQtYWZmaXgnO1xuICAgIGlmIChmaXhlZCkge1xuICAgICAgd3JhcEVsLmNsYXNzTGlzdC5hZGQoY2xzKTtcbiAgICB9IGVsc2Uge1xuICAgICAgd3JhcEVsLmNsYXNzTGlzdC5yZW1vdmUoY2xzKTtcbiAgICB9XG5cbiAgICBpZiAoKGFmZml4U3R5bGUgJiYgIW9yaWdpbmFsQWZmaXhTdHlsZSkgfHwgKCFhZmZpeFN0eWxlICYmIG9yaWdpbmFsQWZmaXhTdHlsZSkpIHtcbiAgICAgIHRoaXMubnpDaGFuZ2UuZW1pdChmaXhlZCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBzZXRQbGFjZWhvbGRlclN0eWxlKHBsYWNlaG9sZGVyU3R5bGU/OiBOR1N0eWxlSW50ZXJmYWNlKTogdm9pZCB7XG4gICAgY29uc3Qgb3JpZ2luYWxQbGFjZWhvbGRlclN0eWxlID0gdGhpcy5wbGFjZWhvbGRlclN0eWxlO1xuICAgIGlmIChzaGFsbG93RXF1YWwocGxhY2Vob2xkZXJTdHlsZSwgb3JpZ2luYWxQbGFjZWhvbGRlclN0eWxlKSkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cbiAgICB0aGlzLnBsYWNlaG9sZGVyTm9kZS5zdHlsZS5jc3NUZXh0ID0gdGhpcy5nZW5TdHlsZShwbGFjZWhvbGRlclN0eWxlKTtcbiAgICB0aGlzLnBsYWNlaG9sZGVyU3R5bGUgPSBwbGFjZWhvbGRlclN0eWxlO1xuICB9XG5cbiAgcHJpdmF0ZSBzeW5jUGxhY2Vob2xkZXJTdHlsZShlOiBFdmVudCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5hZmZpeFN0eWxlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMucGxhY2Vob2xkZXJOb2RlLnN0eWxlLmNzc1RleHQgPSAnJztcbiAgICB0aGlzLnBsYWNlaG9sZGVyU3R5bGUgPSB1bmRlZmluZWQ7XG4gICAgY29uc3Qgc3R5bGVPYmogPSB7IHdpZHRoOiB0aGlzLnBsYWNlaG9sZGVyTm9kZS5vZmZzZXRXaWR0aCwgaGVpZ2h0OiB0aGlzLmZpeGVkRWwubmF0aXZlRWxlbWVudC5vZmZzZXRIZWlnaHQgfTtcbiAgICB0aGlzLnNldEFmZml4U3R5bGUoZSwge1xuICAgICAgLi4udGhpcy5hZmZpeFN0eWxlLFxuICAgICAgLi4uc3R5bGVPYmpcbiAgICB9KTtcbiAgICB0aGlzLnNldFBsYWNlaG9sZGVyU3R5bGUoc3R5bGVPYmopO1xuICB9XG5cbiAgQHRocm90dGxlQnlBbmltYXRpb25GcmFtZURlY29yYXRvcigpXG4gIHVwZGF0ZVBvc2l0aW9uKGU6IEV2ZW50KTogdm9pZCB7XG4gICAgY29uc3QgdGFyZ2V0Tm9kZSA9IHRoaXMuX3RhcmdldDtcbiAgICAvLyBCYWNrd2FyZHMgc3VwcG9ydFxuICAgIGxldCBvZmZzZXRUb3AgPSB0aGlzLm56T2Zmc2V0VG9wO1xuICAgIGNvbnN0IHNjcm9sbFRvcCA9IHRoaXMuc2Nyb2xsU3J2LmdldFNjcm9sbCh0YXJnZXROb2RlLCB0cnVlKTtcbiAgICBjb25zdCBlbGVtT2Zmc2V0ID0gdGhpcy5nZXRPZmZzZXQodGhpcy5wbGFjZWhvbGRlck5vZGUsIHRhcmdldE5vZGUpO1xuICAgIGNvbnN0IGZpeGVkTm9kZSA9IHRoaXMuZml4ZWRFbC5uYXRpdmVFbGVtZW50IGFzIEhUTUxFbGVtZW50O1xuICAgIGNvbnN0IGVsZW1TaXplID0ge1xuICAgICAgd2lkdGg6IGZpeGVkTm9kZS5vZmZzZXRXaWR0aCxcbiAgICAgIGhlaWdodDogZml4ZWROb2RlLm9mZnNldEhlaWdodFxuICAgIH07XG4gICAgY29uc3Qgb2Zmc2V0TW9kZSA9IHtcbiAgICAgIHRvcDogZmFsc2UsXG4gICAgICBib3R0b206IGZhbHNlXG4gICAgfTtcbiAgICAvLyBEZWZhdWx0IHRvIGBvZmZzZXRUb3A9MGAuXG4gICAgaWYgKHR5cGVvZiBvZmZzZXRUb3AgIT09ICdudW1iZXInICYmIHR5cGVvZiB0aGlzLl9vZmZzZXRCb3R0b20gIT09ICdudW1iZXInKSB7XG4gICAgICBvZmZzZXRNb2RlLnRvcCA9IHRydWU7XG4gICAgICBvZmZzZXRUb3AgPSAwO1xuICAgIH0gZWxzZSB7XG4gICAgICBvZmZzZXRNb2RlLnRvcCA9IHR5cGVvZiBvZmZzZXRUb3AgPT09ICdudW1iZXInO1xuICAgICAgb2Zmc2V0TW9kZS5ib3R0b20gPSB0eXBlb2YgdGhpcy5fb2Zmc2V0Qm90dG9tID09PSAnbnVtYmVyJztcbiAgICB9XG4gICAgY29uc3QgdGFyZ2V0UmVjdCA9IHRoaXMuZ2V0VGFyZ2V0UmVjdCh0YXJnZXROb2RlKTtcbiAgICBjb25zdCB0YXJnZXRJbm5lckhlaWdodCA9ICh0YXJnZXROb2RlIGFzIFdpbmRvdykuaW5uZXJIZWlnaHQgfHwgKHRhcmdldE5vZGUgYXMgSFRNTEVsZW1lbnQpLmNsaWVudEhlaWdodDtcbiAgICBpZiAoc2Nyb2xsVG9wID49IGVsZW1PZmZzZXQudG9wIC0gKG9mZnNldFRvcCBhcyBudW1iZXIpICYmIG9mZnNldE1vZGUudG9wKSB7XG4gICAgICBjb25zdCB3aWR0aCA9IGVsZW1PZmZzZXQud2lkdGg7XG4gICAgICBjb25zdCB0b3AgPSB0YXJnZXRSZWN0LnRvcCArIChvZmZzZXRUb3AgYXMgbnVtYmVyKTtcbiAgICAgIHRoaXMuc2V0QWZmaXhTdHlsZShlLCB7XG4gICAgICAgIHBvc2l0aW9uOiAnZml4ZWQnLFxuICAgICAgICB0b3AsXG4gICAgICAgIGxlZnQ6IHRhcmdldFJlY3QubGVmdCArIGVsZW1PZmZzZXQubGVmdCxcbiAgICAgICAgbWF4SGVpZ2h0OiBgY2FsYygxMDB2aCAtICR7dG9wfXB4KWAsXG4gICAgICAgIHdpZHRoXG4gICAgICB9KTtcbiAgICAgIHRoaXMuc2V0UGxhY2Vob2xkZXJTdHlsZSh7XG4gICAgICAgIHdpZHRoLFxuICAgICAgICBoZWlnaHQ6IGVsZW1TaXplLmhlaWdodFxuICAgICAgfSk7XG4gICAgfSBlbHNlIGlmIChcbiAgICAgIHNjcm9sbFRvcCA8PSBlbGVtT2Zmc2V0LnRvcCArIGVsZW1TaXplLmhlaWdodCArICh0aGlzLl9vZmZzZXRCb3R0b20gYXMgbnVtYmVyKSAtIHRhcmdldElubmVySGVpZ2h0ICYmXG4gICAgICBvZmZzZXRNb2RlLmJvdHRvbVxuICAgICkge1xuICAgICAgY29uc3QgdGFyZ2V0Qm90dG9tT2ZmZXQgPSB0YXJnZXROb2RlID09PSB3aW5kb3cgPyAwIDogd2luZG93LmlubmVySGVpZ2h0IC0gdGFyZ2V0UmVjdC5ib3R0b207XG4gICAgICBjb25zdCB3aWR0aCA9IGVsZW1PZmZzZXQud2lkdGg7XG4gICAgICB0aGlzLnNldEFmZml4U3R5bGUoZSwge1xuICAgICAgICBwb3NpdGlvbjogJ2ZpeGVkJyxcbiAgICAgICAgYm90dG9tOiB0YXJnZXRCb3R0b21PZmZldCArICh0aGlzLl9vZmZzZXRCb3R0b20gYXMgbnVtYmVyKSxcbiAgICAgICAgbGVmdDogdGFyZ2V0UmVjdC5sZWZ0ICsgZWxlbU9mZnNldC5sZWZ0LFxuICAgICAgICB3aWR0aFxuICAgICAgfSk7XG4gICAgICB0aGlzLnNldFBsYWNlaG9sZGVyU3R5bGUoe1xuICAgICAgICB3aWR0aCxcbiAgICAgICAgaGVpZ2h0OiBlbGVtT2Zmc2V0LmhlaWdodFxuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChcbiAgICAgICAgZS50eXBlID09PSAncmVzaXplJyAmJlxuICAgICAgICB0aGlzLmFmZml4U3R5bGUgJiZcbiAgICAgICAgdGhpcy5hZmZpeFN0eWxlLnBvc2l0aW9uID09PSAnZml4ZWQnICYmXG4gICAgICAgIHRoaXMucGxhY2Vob2xkZXJOb2RlLm9mZnNldFdpZHRoXG4gICAgICApIHtcbiAgICAgICAgdGhpcy5zZXRBZmZpeFN0eWxlKGUsIHsgLi4udGhpcy5hZmZpeFN0eWxlLCB3aWR0aDogdGhpcy5wbGFjZWhvbGRlck5vZGUub2Zmc2V0V2lkdGggfSk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLnNldEFmZml4U3R5bGUoZSk7XG4gICAgICB9XG4gICAgICB0aGlzLnNldFBsYWNlaG9sZGVyU3R5bGUoKTtcbiAgICB9XG5cbiAgICBpZiAoZS50eXBlID09PSAncmVzaXplJykge1xuICAgICAgdGhpcy5zeW5jUGxhY2Vob2xkZXJTdHlsZShlKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==