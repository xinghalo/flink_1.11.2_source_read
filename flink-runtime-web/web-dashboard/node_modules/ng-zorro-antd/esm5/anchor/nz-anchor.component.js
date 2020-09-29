/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { DOCUMENT } from '@angular/common';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Inject, Input, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { fromEvent } from 'rxjs';
import { distinctUntilChanged, throttleTime } from 'rxjs/operators';
import { NzScrollService } from '../core/scroll/nz-scroll.service';
import { toNumber, InputBoolean, InputNumber } from '../core/util/convert';
/**
 * @record
 */
function Section() { }
if (false) {
    /** @type {?} */
    Section.prototype.comp;
    /** @type {?} */
    Section.prototype.top;
}
/** @type {?} */
var sharpMatcherRegx = /#([^#]+)$/;
var NzAnchorComponent = /** @class */ (function () {
    /* tslint:disable-next-line:no-any */
    function NzAnchorComponent(scrollSrv, doc, cdr) {
        this.scrollSrv = scrollSrv;
        this.doc = doc;
        this.cdr = cdr;
        this.nzAffix = true;
        this.nzShowInkInFixed = false;
        this.nzBounds = 5;
        this.nzClick = new EventEmitter();
        this.nzScroll = new EventEmitter();
        this.visible = false;
        this.wrapperStyle = { 'max-height': '100vh' };
        this.links = [];
        this.animating = false;
        this.target = null;
        this.scroll$ = null;
        this.destroyed = false;
    }
    Object.defineProperty(NzAnchorComponent.prototype, "nzOffsetTop", {
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
            this._offsetTop = toNumber(value, 0);
            this.wrapperStyle = {
                'max-height': "calc(100vh - " + this._offsetTop + "px)"
            };
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzAnchorComponent.prototype, "nzTarget", {
        set: /**
         * @param {?} el
         * @return {?}
         */
        function (el) {
            this.target = typeof el === 'string' ? this.doc.querySelector(el) : el;
            this.registerScrollEvent();
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @param {?} link
     * @return {?}
     */
    NzAnchorComponent.prototype.registerLink = /**
     * @param {?} link
     * @return {?}
     */
    function (link) {
        this.links.push(link);
    };
    /**
     * @param {?} link
     * @return {?}
     */
    NzAnchorComponent.prototype.unregisterLink = /**
     * @param {?} link
     * @return {?}
     */
    function (link) {
        this.links.splice(this.links.indexOf(link), 1);
    };
    /**
     * @private
     * @return {?}
     */
    NzAnchorComponent.prototype.getTarget = /**
     * @private
     * @return {?}
     */
    function () {
        return this.target || window;
    };
    /**
     * @return {?}
     */
    NzAnchorComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        this.registerScrollEvent();
    };
    /**
     * @return {?}
     */
    NzAnchorComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.destroyed = true;
        this.removeListen();
    };
    /**
     * @private
     * @return {?}
     */
    NzAnchorComponent.prototype.registerScrollEvent = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        this.removeListen();
        this.scroll$ = fromEvent(this.getTarget(), 'scroll')
            .pipe(throttleTime(50), distinctUntilChanged())
            .subscribe((/**
         * @return {?}
         */
        function () { return _this.handleScroll(); }));
        // 浏览器在刷新时保持滚动位置，会倒置在dom未渲染完成时计算不正确，因此延迟重新计算
        // 与之相对应可能会引起组件移除后依然触发 `handleScroll` 的 `detectChanges`
        setTimeout((/**
         * @return {?}
         */
        function () { return _this.handleScroll(); }));
    };
    /**
     * @private
     * @return {?}
     */
    NzAnchorComponent.prototype.removeListen = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.scroll$) {
            this.scroll$.unsubscribe();
        }
    };
    /**
     * @private
     * @param {?} element
     * @return {?}
     */
    NzAnchorComponent.prototype.getOffsetTop = /**
     * @private
     * @param {?} element
     * @return {?}
     */
    function (element) {
        if (!element || !element.getClientRects().length) {
            return 0;
        }
        /** @type {?} */
        var rect = element.getBoundingClientRect();
        if (!rect.width && !rect.height) {
            return rect.top;
        }
        return rect.top - (/** @type {?} */ ((/** @type {?} */ (element.ownerDocument)).documentElement)).clientTop;
    };
    /**
     * @return {?}
     */
    NzAnchorComponent.prototype.handleScroll = /**
     * @return {?}
     */
    function () {
        var _this = this;
        if (this.destroyed || this.animating) {
            return;
        }
        /** @type {?} */
        var sections = [];
        /** @type {?} */
        var scope = (this.nzOffsetTop || 0) + this.nzBounds;
        this.links.forEach((/**
         * @param {?} comp
         * @return {?}
         */
        function (comp) {
            /** @type {?} */
            var sharpLinkMatch = sharpMatcherRegx.exec(comp.nzHref.toString());
            if (!sharpLinkMatch) {
                return;
            }
            /** @type {?} */
            var target = _this.doc.getElementById(sharpLinkMatch[1]);
            if (target && _this.getOffsetTop(target) < scope) {
                /** @type {?} */
                var top_1 = _this.getOffsetTop(target);
                sections.push({
                    top: top_1,
                    comp: comp
                });
            }
        }));
        this.visible = !!sections.length;
        if (!this.visible) {
            this.clearActive();
            this.cdr.detectChanges();
        }
        else {
            /** @type {?} */
            var maxSection = sections.reduce((/**
             * @param {?} prev
             * @param {?} curr
             * @return {?}
             */
            function (prev, curr) { return (curr.top > prev.top ? curr : prev); }));
            this.handleActive(maxSection.comp);
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzAnchorComponent.prototype.clearActive = /**
     * @private
     * @return {?}
     */
    function () {
        this.links.forEach((/**
         * @param {?} i
         * @return {?}
         */
        function (i) {
            i.active = false;
            i.markForCheck();
        }));
    };
    /**
     * @private
     * @param {?} comp
     * @return {?}
     */
    NzAnchorComponent.prototype.handleActive = /**
     * @private
     * @param {?} comp
     * @return {?}
     */
    function (comp) {
        this.clearActive();
        comp.active = true;
        comp.markForCheck();
        /** @type {?} */
        var linkNode = (/** @type {?} */ (((/** @type {?} */ (comp.elementRef.nativeElement))).querySelector('.ant-anchor-link-title')));
        this.ink.nativeElement.style.top = linkNode.offsetTop + linkNode.clientHeight / 2 - 4.5 + "px";
        this.cdr.detectChanges();
        this.nzScroll.emit(comp);
    };
    /**
     * @param {?} linkComp
     * @return {?}
     */
    NzAnchorComponent.prototype.handleScrollTo = /**
     * @param {?} linkComp
     * @return {?}
     */
    function (linkComp) {
        var _this = this;
        /** @type {?} */
        var el = this.doc.querySelector(linkComp.nzHref);
        if (!el) {
            return;
        }
        this.animating = true;
        /** @type {?} */
        var containerScrollTop = this.scrollSrv.getScroll(this.getTarget());
        /** @type {?} */
        var elOffsetTop = this.scrollSrv.getOffset(el).top;
        /** @type {?} */
        var targetScrollTop = containerScrollTop + elOffsetTop - (this.nzOffsetTop || 0);
        this.scrollSrv.scrollTo(this.getTarget(), targetScrollTop, undefined, (/**
         * @return {?}
         */
        function () {
            _this.animating = false;
            _this.handleActive(linkComp);
        }));
        this.nzClick.emit(linkComp.nzHref);
    };
    NzAnchorComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-anchor',
                    preserveWhitespaces: false,
                    template: "<nz-affix *ngIf=\"nzAffix;else content\" [nzOffsetTop]=\"nzOffsetTop\">\n  <ng-template [ngTemplateOutlet]=\"content\"></ng-template>\n</nz-affix>\n<ng-template #content>\n  <div class=\"ant-anchor-wrapper\" [ngStyle]=\"wrapperStyle\">\n    <div class=\"ant-anchor\" [ngClass]=\"{'fixed': !nzAffix && !nzShowInkInFixed}\">\n      <div class=\"ant-anchor-ink\">\n        <div class=\"ant-anchor-ink-ball\" [class.visible]=\"visible\" #ink></div>\n      </div>\n      <ng-content></ng-content>\n    </div>\n  </div>\n</ng-template>",
                    encapsulation: ViewEncapsulation.None,
                    changeDetection: ChangeDetectionStrategy.OnPush
                }] }
    ];
    /** @nocollapse */
    NzAnchorComponent.ctorParameters = function () { return [
        { type: NzScrollService },
        { type: undefined, decorators: [{ type: Inject, args: [DOCUMENT,] }] },
        { type: ChangeDetectorRef }
    ]; };
    NzAnchorComponent.propDecorators = {
        ink: [{ type: ViewChild, args: ['ink',] }],
        nzAffix: [{ type: Input }],
        nzShowInkInFixed: [{ type: Input }],
        nzBounds: [{ type: Input }],
        nzOffsetTop: [{ type: Input }],
        nzTarget: [{ type: Input }],
        nzClick: [{ type: Output }],
        nzScroll: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzAnchorComponent.prototype, "nzAffix", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzAnchorComponent.prototype, "nzShowInkInFixed", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Number)
    ], NzAnchorComponent.prototype, "nzBounds", void 0);
    return NzAnchorComponent;
}());
export { NzAnchorComponent };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.ink;
    /** @type {?} */
    NzAnchorComponent.prototype.nzAffix;
    /** @type {?} */
    NzAnchorComponent.prototype.nzShowInkInFixed;
    /** @type {?} */
    NzAnchorComponent.prototype.nzBounds;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype._offsetTop;
    /** @type {?} */
    NzAnchorComponent.prototype.nzClick;
    /** @type {?} */
    NzAnchorComponent.prototype.nzScroll;
    /** @type {?} */
    NzAnchorComponent.prototype.visible;
    /** @type {?} */
    NzAnchorComponent.prototype.wrapperStyle;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.links;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.animating;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.target;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.scroll$;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.destroyed;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.scrollSrv;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.doc;
    /**
     * @type {?}
     * @private
     */
    NzAnchorComponent.prototype.cdr;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYW5jaG9yLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJhbmNob3IvbnotYW5jaG9yLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMzQyxPQUFPLEVBRUwsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsVUFBVSxFQUNWLFlBQVksRUFDWixNQUFNLEVBQ04sS0FBSyxFQUVMLE1BQU0sRUFDTixTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxTQUFTLEVBQWdCLE1BQU0sTUFBTSxDQUFDO0FBQy9DLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxZQUFZLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUNwRSxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sa0NBQWtDLENBQUM7QUFFbkUsT0FBTyxFQUFFLFFBQVEsRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7Ozs7QUFJM0Usc0JBR0M7OztJQUZDLHVCQUE0Qjs7SUFDNUIsc0JBQVk7OztJQUdSLGdCQUFnQixHQUFHLFdBQVc7QUFFcEM7SUE4Q0UscUNBQXFDO0lBQ3JDLDJCQUFvQixTQUEwQixFQUE0QixHQUFRLEVBQVUsR0FBc0I7UUFBOUYsY0FBUyxHQUFULFNBQVMsQ0FBaUI7UUFBNEIsUUFBRyxHQUFILEdBQUcsQ0FBSztRQUFVLFFBQUcsR0FBSCxHQUFHLENBQW1CO1FBckN6RixZQUFPLEdBQUcsSUFBSSxDQUFDO1FBQ2YscUJBQWdCLEdBQUcsS0FBSyxDQUFDO1FBQzFCLGFBQVEsR0FBVyxDQUFDLENBQUM7UUFzQjFCLFlBQU8sR0FBRyxJQUFJLFlBQVksRUFBVSxDQUFDO1FBQ3JDLGFBQVEsR0FBRyxJQUFJLFlBQVksRUFBeUIsQ0FBQztRQUV4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aEQ7Ozs7Z0JBbkJRLGVBQWU7Z0RBNEQyQixNQUFNLFNBQUMsUUFBUTtnQkF6RWhFLGlCQUFpQjs7O3NCQWtDaEIsU0FBUyxTQUFDLEtBQUs7MEJBRWYsS0FBSzttQ0FDTCxLQUFLOzJCQUNMLEtBQUs7OEJBRUwsS0FBSzsyQkFjTCxLQUFLOzBCQU1MLE1BQU07MkJBQ04sTUFBTTs7SUF6QmtCO1FBQWYsWUFBWSxFQUFFOztzREFBZ0I7SUFDZjtRQUFmLFlBQVksRUFBRTs7K0RBQTBCO0lBQzFCO1FBQWQsV0FBVyxFQUFFOzt1REFBc0I7SUE4Si9DLHdCQUFDO0NBQUEsQUExS0QsSUEwS0M7U0FuS1ksaUJBQWlCOzs7Ozs7SUFDNUIsZ0NBQTBDOztJQUUxQyxvQ0FBd0M7O0lBQ3hDLDZDQUFrRDs7SUFDbEQscUNBQTZDOzs7OztJQWM3Qyx1Q0FBMkI7O0lBUTNCLG9DQUF3RDs7SUFDeEQscUNBQXdFOztJQUV4RSxvQ0FBZ0I7O0lBQ2hCLHlDQUEyRDs7Ozs7SUFFM0Qsa0NBQTRDOzs7OztJQUM1QyxzQ0FBMEI7Ozs7O0lBQzFCLG1DQUFzQzs7Ozs7SUFDdEMsb0NBQTRDOzs7OztJQUM1QyxzQ0FBMEI7Ozs7O0lBR2Qsc0NBQWtDOzs7OztJQUFFLGdDQUFrQzs7Ozs7SUFBRSxnQ0FBOEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBET0NVTUVOVCB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQge1xuICBBZnRlclZpZXdJbml0LFxuICBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneSxcbiAgQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gIENvbXBvbmVudCxcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbmplY3QsXG4gIElucHV0LFxuICBPbkRlc3Ryb3ksXG4gIE91dHB1dCxcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcbmltcG9ydCB7IGZyb21FdmVudCwgU3Vic2NyaXB0aW9uIH0gZnJvbSAncnhqcyc7XG5pbXBvcnQgeyBkaXN0aW5jdFVudGlsQ2hhbmdlZCwgdGhyb3R0bGVUaW1lIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgTnpTY3JvbGxTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zY3JvbGwvbnotc2Nyb2xsLnNlcnZpY2UnO1xuaW1wb3J0IHsgTkdTdHlsZUludGVyZmFjZSB9IGZyb20gJy4uL2NvcmUvdHlwZXMvbmctY2xhc3MnO1xuaW1wb3J0IHsgdG9OdW1iZXIsIElucHV0Qm9vbGVhbiwgSW5wdXROdW1iZXIgfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5cbmltcG9ydCB7IE56QW5jaG9yTGlua0NvbXBvbmVudCB9IGZyb20gJy4vbnotYW5jaG9yLWxpbmsuY29tcG9uZW50JztcblxuaW50ZXJmYWNlIFNlY3Rpb24ge1xuICBjb21wOiBOekFuY2hvckxpbmtDb21wb25lbnQ7XG4gIHRvcDogbnVtYmVyO1xufVxuXG5jb25zdCBzaGFycE1hdGNoZXJSZWd4ID0gLyMoW14jXSspJC87XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LWFuY2hvcicsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotYW5jaG9yLmNvbXBvbmVudC5odG1sJyxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2hcbn0pXG5leHBvcnQgY2xhc3MgTnpBbmNob3JDb21wb25lbnQgaW1wbGVtZW50cyBPbkRlc3Ryb3ksIEFmdGVyVmlld0luaXQge1xuICBAVmlld0NoaWxkKCdpbmsnKSBwcml2YXRlIGluazogRWxlbWVudFJlZjtcblxuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpBZmZpeCA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelNob3dJbmtJbkZpeGVkID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56Qm91bmRzOiBudW1iZXIgPSA1O1xuXG4gIEBJbnB1dCgpXG4gIHNldCBuek9mZnNldFRvcCh2YWx1ZTogbnVtYmVyKSB7XG4gICAgdGhpcy5fb2Zmc2V0VG9wID0gdG9OdW1iZXIodmFsdWUsIDApO1xuICAgIHRoaXMud3JhcHBlclN0eWxlID0ge1xuICAgICAgJ21heC1oZWlnaHQnOiBgY2FsYygxMDB2aCAtICR7dGhpcy5fb2Zmc2V0VG9wfXB4KWBcbiAgICB9O1xuICB9XG5cbiAgZ2V0IG56T2Zmc2V0VG9wKCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuX29mZnNldFRvcDtcbiAgfVxuXG4gIHByaXZhdGUgX29mZnNldFRvcDogbnVtYmVyO1xuXG4gIEBJbnB1dCgpXG4gIHNldCBuelRhcmdldChlbDogc3RyaW5nIHwgRWxlbWVudCkge1xuICAgIHRoaXMudGFyZ2V0ID0gdHlwZW9mIGVsID09PSAnc3RyaW5nJyA/IHRoaXMuZG9jLnF1ZXJ5U2VsZWN0b3IoZWwpIDogZWw7XG4gICAgdGhpcy5yZWdpc3RlclNjcm9sbEV2ZW50KCk7XG4gIH1cblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpDbGljayA9IG5ldyBFdmVudEVtaXR0ZXI8c3RyaW5nPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpTY3JvbGwgPSBuZXcgRXZlbnRFbWl0dGVyPE56QW5jaG9yTGlua0NvbXBvbmVudD4oKTtcblxuICB2aXNpYmxlID0gZmFsc2U7XG4gIHdyYXBwZXJTdHlsZTogTkdTdHlsZUludGVyZmFjZSA9IHsgJ21heC1oZWlnaHQnOiAnMTAwdmgnIH07XG5cbiAgcHJpdmF0ZSBsaW5rczogTnpBbmNob3JMaW5rQ29tcG9uZW50W10gPSBbXTtcbiAgcHJpdmF0ZSBhbmltYXRpbmcgPSBmYWxzZTtcbiAgcHJpdmF0ZSB0YXJnZXQ6IEVsZW1lbnQgfCBudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBzY3JvbGwkOiBTdWJzY3JpcHRpb24gfCBudWxsID0gbnVsbDtcbiAgcHJpdmF0ZSBkZXN0cm95ZWQgPSBmYWxzZTtcblxuICAvKiB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55ICovXG4gIGNvbnN0cnVjdG9yKHByaXZhdGUgc2Nyb2xsU3J2OiBOelNjcm9sbFNlcnZpY2UsIEBJbmplY3QoRE9DVU1FTlQpIHByaXZhdGUgZG9jOiBhbnksIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZikge31cblxuICByZWdpc3RlckxpbmsobGluazogTnpBbmNob3JMaW5rQ29tcG9uZW50KTogdm9pZCB7XG4gICAgdGhpcy5saW5rcy5wdXNoKGxpbmspO1xuICB9XG5cbiAgdW5yZWdpc3RlckxpbmsobGluazogTnpBbmNob3JMaW5rQ29tcG9uZW50KTogdm9pZCB7XG4gICAgdGhpcy5saW5rcy5zcGxpY2UodGhpcy5saW5rcy5pbmRleE9mKGxpbmspLCAxKTtcbiAgfVxuXG4gIHByaXZhdGUgZ2V0VGFyZ2V0KCk6IEVsZW1lbnQgfCBXaW5kb3cge1xuICAgIHJldHVybiB0aGlzLnRhcmdldCB8fCB3aW5kb3c7XG4gIH1cblxuICBuZ0FmdGVyVmlld0luaXQoKTogdm9pZCB7XG4gICAgdGhpcy5yZWdpc3RlclNjcm9sbEV2ZW50KCk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3llZCA9IHRydWU7XG4gICAgdGhpcy5yZW1vdmVMaXN0ZW4oKTtcbiAgfVxuXG4gIHByaXZhdGUgcmVnaXN0ZXJTY3JvbGxFdmVudCgpOiB2b2lkIHtcbiAgICB0aGlzLnJlbW92ZUxpc3RlbigpO1xuICAgIHRoaXMuc2Nyb2xsJCA9IGZyb21FdmVudCh0aGlzLmdldFRhcmdldCgpLCAnc2Nyb2xsJylcbiAgICAgIC5waXBlKFxuICAgICAgICB0aHJvdHRsZVRpbWUoNTApLFxuICAgICAgICBkaXN0aW5jdFVudGlsQ2hhbmdlZCgpXG4gICAgICApXG4gICAgICAuc3Vic2NyaWJlKCgpID0+IHRoaXMuaGFuZGxlU2Nyb2xsKCkpO1xuICAgIC8vIOa1j+iniOWZqOWcqOWIt+aWsOaXtuS/neaMgea7muWKqOS9jee9ru+8jOS8muWAkue9ruWcqGRvbeacqua4suafk+WujOaIkOaXtuiuoeeul+S4jeato+ehru+8jOWboOatpOW7tui/n+mHjeaWsOiuoeeul1xuICAgIC8vIOS4juS5i+ebuOWvueW6lOWPr+iDveS8muW8lei1t+e7hOS7tuenu+mZpOWQjuS+neeEtuinpuWPkSBgaGFuZGxlU2Nyb2xsYCDnmoQgYGRldGVjdENoYW5nZXNgXG4gICAgc2V0VGltZW91dCgoKSA9PiB0aGlzLmhhbmRsZVNjcm9sbCgpKTtcbiAgfVxuXG4gIHByaXZhdGUgcmVtb3ZlTGlzdGVuKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnNjcm9sbCQpIHtcbiAgICAgIHRoaXMuc2Nyb2xsJC51bnN1YnNjcmliZSgpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgZ2V0T2Zmc2V0VG9wKGVsZW1lbnQ6IEhUTUxFbGVtZW50KTogbnVtYmVyIHtcbiAgICBpZiAoIWVsZW1lbnQgfHwgIWVsZW1lbnQuZ2V0Q2xpZW50UmVjdHMoKS5sZW5ndGgpIHtcbiAgICAgIHJldHVybiAwO1xuICAgIH1cbiAgICBjb25zdCByZWN0ID0gZWxlbWVudC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKTtcbiAgICBpZiAoIXJlY3Qud2lkdGggJiYgIXJlY3QuaGVpZ2h0KSB7XG4gICAgICByZXR1cm4gcmVjdC50b3A7XG4gICAgfVxuICAgIHJldHVybiByZWN0LnRvcCAtIGVsZW1lbnQub3duZXJEb2N1bWVudCEuZG9jdW1lbnRFbGVtZW50IS5jbGllbnRUb3A7XG4gIH1cblxuICBoYW5kbGVTY3JvbGwoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuZGVzdHJveWVkIHx8IHRoaXMuYW5pbWF0aW5nKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuXG4gICAgY29uc3Qgc2VjdGlvbnM6IFNlY3Rpb25bXSA9IFtdO1xuICAgIGNvbnN0IHNjb3BlID0gKHRoaXMubnpPZmZzZXRUb3AgfHwgMCkgKyB0aGlzLm56Qm91bmRzO1xuICAgIHRoaXMubGlua3MuZm9yRWFjaChjb21wID0+IHtcbiAgICAgIGNvbnN0IHNoYXJwTGlua01hdGNoID0gc2hhcnBNYXRjaGVyUmVneC5leGVjKGNvbXAubnpIcmVmLnRvU3RyaW5nKCkpO1xuICAgICAgaWYgKCFzaGFycExpbmtNYXRjaCkge1xuICAgICAgICByZXR1cm47XG4gICAgICB9XG4gICAgICBjb25zdCB0YXJnZXQgPSB0aGlzLmRvYy5nZXRFbGVtZW50QnlJZChzaGFycExpbmtNYXRjaFsxXSk7XG4gICAgICBpZiAodGFyZ2V0ICYmIHRoaXMuZ2V0T2Zmc2V0VG9wKHRhcmdldCkgPCBzY29wZSkge1xuICAgICAgICBjb25zdCB0b3AgPSB0aGlzLmdldE9mZnNldFRvcCh0YXJnZXQpO1xuICAgICAgICBzZWN0aW9ucy5wdXNoKHtcbiAgICAgICAgICB0b3AsXG4gICAgICAgICAgY29tcFxuICAgICAgICB9KTtcbiAgICAgIH1cbiAgICB9KTtcblxuICAgIHRoaXMudmlzaWJsZSA9ICEhc2VjdGlvbnMubGVuZ3RoO1xuICAgIGlmICghdGhpcy52aXNpYmxlKSB7XG4gICAgICB0aGlzLmNsZWFyQWN0aXZlKCk7XG4gICAgICB0aGlzLmNkci5kZXRlY3RDaGFuZ2VzKCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIGNvbnN0IG1heFNlY3Rpb24gPSBzZWN0aW9ucy5yZWR1Y2UoKHByZXYsIGN1cnIpID0+IChjdXJyLnRvcCA+IHByZXYudG9wID8gY3VyciA6IHByZXYpKTtcbiAgICAgIHRoaXMuaGFuZGxlQWN0aXZlKG1heFNlY3Rpb24uY29tcCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBjbGVhckFjdGl2ZSgpOiB2b2lkIHtcbiAgICB0aGlzLmxpbmtzLmZvckVhY2goaSA9PiB7XG4gICAgICBpLmFjdGl2ZSA9IGZhbHNlO1xuICAgICAgaS5tYXJrRm9yQ2hlY2soKTtcbiAgICB9KTtcbiAgfVxuXG4gIHByaXZhdGUgaGFuZGxlQWN0aXZlKGNvbXA6IE56QW5jaG9yTGlua0NvbXBvbmVudCk6IHZvaWQge1xuICAgIHRoaXMuY2xlYXJBY3RpdmUoKTtcblxuICAgIGNvbXAuYWN0aXZlID0gdHJ1ZTtcbiAgICBjb21wLm1hcmtGb3JDaGVjaygpO1xuXG4gICAgY29uc3QgbGlua05vZGUgPSAoY29tcC5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQgYXMgSFRNTERpdkVsZW1lbnQpLnF1ZXJ5U2VsZWN0b3IoXG4gICAgICAnLmFudC1hbmNob3ItbGluay10aXRsZSdcbiAgICApIGFzIEhUTUxFbGVtZW50O1xuICAgIHRoaXMuaW5rLm5hdGl2ZUVsZW1lbnQuc3R5bGUudG9wID0gYCR7bGlua05vZGUub2Zmc2V0VG9wICsgbGlua05vZGUuY2xpZW50SGVpZ2h0IC8gMiAtIDQuNX1weGA7XG4gICAgdGhpcy5jZHIuZGV0ZWN0Q2hhbmdlcygpO1xuXG4gICAgdGhpcy5uelNjcm9sbC5lbWl0KGNvbXApO1xuICB9XG5cbiAgaGFuZGxlU2Nyb2xsVG8obGlua0NvbXA6IE56QW5jaG9yTGlua0NvbXBvbmVudCk6IHZvaWQge1xuICAgIGNvbnN0IGVsID0gdGhpcy5kb2MucXVlcnlTZWxlY3RvcihsaW5rQ29tcC5uekhyZWYpO1xuICAgIGlmICghZWwpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICB0aGlzLmFuaW1hdGluZyA9IHRydWU7XG4gICAgY29uc3QgY29udGFpbmVyU2Nyb2xsVG9wID0gdGhpcy5zY3JvbGxTcnYuZ2V0U2Nyb2xsKHRoaXMuZ2V0VGFyZ2V0KCkpO1xuICAgIGNvbnN0IGVsT2Zmc2V0VG9wID0gdGhpcy5zY3JvbGxTcnYuZ2V0T2Zmc2V0KGVsKS50b3A7XG4gICAgY29uc3QgdGFyZ2V0U2Nyb2xsVG9wID0gY29udGFpbmVyU2Nyb2xsVG9wICsgZWxPZmZzZXRUb3AgLSAodGhpcy5uek9mZnNldFRvcCB8fCAwKTtcbiAgICB0aGlzLnNjcm9sbFNydi5zY3JvbGxUbyh0aGlzLmdldFRhcmdldCgpLCB0YXJnZXRTY3JvbGxUb3AsIHVuZGVmaW5lZCwgKCkgPT4ge1xuICAgICAgdGhpcy5hbmltYXRpbmcgPSBmYWxzZTtcbiAgICAgIHRoaXMuaGFuZGxlQWN0aXZlKGxpbmtDb21wKTtcbiAgICB9KTtcbiAgICB0aGlzLm56Q2xpY2suZW1pdChsaW5rQ29tcC5uekhyZWYpO1xuICB9XG59XG4iXX0=