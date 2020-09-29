/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { LEFT_ARROW, RIGHT_ARROW } from '@angular/cdk/keycodes';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChildren, ElementRef, EventEmitter, Input, NgZone, Output, QueryList, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { fromEvent, Subscription } from 'rxjs';
import { debounceTime } from 'rxjs/operators';
import { InputBoolean, InputNumber } from '../core/util/convert';
import { NzCarouselContentDirective } from './nz-carousel-content.directive';
var NzCarouselComponent = /** @class */ (function () {
    function NzCarouselComponent(elementRef, renderer, cdr, ngZone) {
        this.elementRef = elementRef;
        this.renderer = renderer;
        this.cdr = cdr;
        this.ngZone = ngZone;
        this.nzTransitionSpeed = 500; // Not exposed.
        this.nzEffect = 'scrollx';
        this.nzEnableSwipe = true;
        this.nzDots = true;
        this.nzVertical = false;
        this.nzAutoPlay = false;
        this.nzAutoPlaySpeed = 3000; // Should be nzAutoPlayDuration, but changing this is breaking.
        // Should be nzAutoPlayDuration, but changing this is breaking.
        this.nzAfterChange = new EventEmitter();
        this.nzBeforeChange = new EventEmitter();
        this.activeIndex = 0;
        this.transform = 'translate3d(0px, 0px, 0px)';
        this.el = this.elementRef.nativeElement;
        this.subs_ = new Subscription();
        renderer.addClass(elementRef.nativeElement, 'ant-carousel');
    }
    Object.defineProperty(NzCarouselComponent.prototype, "nextIndex", {
        get: /**
         * @return {?}
         */
        function () {
            return this.activeIndex < this.slideContents.length - 1 ? this.activeIndex + 1 : 0;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzCarouselComponent.prototype, "prevIndex", {
        get: /**
         * @return {?}
         */
        function () {
            return this.activeIndex > 0 ? this.activeIndex - 1 : this.slideContents.length - 1;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzCarouselComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        if (this.slideContents && this.slideContents.length) {
            this.slideContents.first.isActive = true;
        }
    };
    /**
     * @return {?}
     */
    NzCarouselComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        // Re-render when content changes.
        this.subs_.add(this.slideContents.changes.subscribe((/**
         * @return {?}
         */
        function () {
            _this.renderContent();
        })));
        this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        function () {
            _this.subs_.add(fromEvent(window, 'resize')
                .pipe(debounceTime(50))
                .subscribe((/**
             * @return {?}
             */
            function () {
                _this.renderContent();
                _this.setTransition();
            })));
        }));
        // When used in modals (drawers maybe too), it should render itself asynchronously.
        // Refer to https://github.com/NG-ZORRO/ng-zorro-antd/issues/2387
        Promise.resolve().then((/**
         * @return {?}
         */
        function () {
            _this.renderContent();
        }));
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzCarouselComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzAutoPlay || changes.nzAutoPlaySpeed) {
            this.setUpNextScroll();
        }
        if (changes.nzEffect) {
            this.updateMode();
        }
    };
    /**
     * @return {?}
     */
    NzCarouselComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.subs_.unsubscribe();
        this.clearTimeout();
    };
    /**
     * @param {?} index
     * @return {?}
     */
    NzCarouselComponent.prototype.setContentActive = /**
     * @param {?} index
     * @return {?}
     */
    function (index) {
        var _this = this;
        if (this.slideContents && this.slideContents.length) {
            this.nzBeforeChange.emit({ from: this.slideContents.toArray().findIndex((/**
                 * @param {?} slide
                 * @return {?}
                 */
                function (slide) { return slide.isActive; })), to: index });
            this.activeIndex = index;
            this.setTransition();
            this.slideContents.forEach((/**
             * @param {?} slide
             * @param {?} i
             * @return {?}
             */
            function (slide, i) { return (slide.isActive = index === i); }));
            this.setUpNextScroll();
            this.cdr.markForCheck();
            // Should trigger the following when animation is done. The transition takes 0.5 seconds according to the CSS.
            setTimeout((/**
             * @return {?}
             */
            function () { return _this.nzAfterChange.emit(index); }), this.nzTransitionSpeed);
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCarouselComponent.prototype.setTransition = /**
     * @private
     * @return {?}
     */
    function () {
        this.transform =
            this.nzEffect === 'fade'
                ? 'translate3d(0px, 0px, 0px)'
                : this.nzVertical
                    ? // `Scrollx` mode.
                        "translate3d(0px, " + -this.activeIndex * this.el.offsetHeight + "px, 0px)"
                    : "translate3d(" + -this.activeIndex * this.el.offsetWidth + "px, 0px, 0px)";
        if (this.slickTrack) {
            this.renderer.setStyle(this.slickTrack.nativeElement, 'transform', this.transform);
        }
    };
    /**
     * @return {?}
     */
    NzCarouselComponent.prototype.next = /**
     * @return {?}
     */
    function () {
        this.setContentActive(this.nextIndex);
    };
    /**
     * @return {?}
     */
    NzCarouselComponent.prototype.pre = /**
     * @return {?}
     */
    function () {
        this.setContentActive(this.prevIndex);
    };
    /**
     * @param {?} index
     * @return {?}
     */
    NzCarouselComponent.prototype.goTo = /**
     * @param {?} index
     * @return {?}
     */
    function (index) {
        if (index >= 0 && index <= this.slideContents.length - 1) {
            this.setContentActive(index);
        }
    };
    /**
     * @param {?} e
     * @return {?}
     */
    NzCarouselComponent.prototype.onKeyDown = /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (e.keyCode === LEFT_ARROW) {
            // Left
            this.pre();
            e.preventDefault();
        }
        else if (e.keyCode === RIGHT_ARROW) {
            // Right
            this.next();
            e.preventDefault();
        }
    };
    /**
     * @param {?=} action
     * @return {?}
     */
    NzCarouselComponent.prototype.swipe = /**
     * @param {?=} action
     * @return {?}
     */
    function (action) {
        if (action === void 0) { action = 'swipeleft'; }
        if (!this.nzEnableSwipe) {
            return;
        }
        if (action === 'swipeleft') {
            this.next();
        }
        if (action === 'swiperight') {
            this.pre();
        }
    };
    /* tslint:disable-next-line:no-any */
    /* tslint:disable-next-line:no-any */
    /**
     * @param {?} e
     * @return {?}
     */
    NzCarouselComponent.prototype.swipeInProgress = /* tslint:disable-next-line:no-any */
    /**
     * @param {?} e
     * @return {?}
     */
    function (e) {
        if (this.nzEffect === 'scrollx') {
            /** @type {?} */
            var final = e.isFinal;
            /** @type {?} */
            var scrollWidth = final ? 0 : e.deltaX * 1.2;
            /** @type {?} */
            var totalWidth = this.el.offsetWidth;
            if (this.nzVertical) {
                /** @type {?} */
                var totalHeight = this.el.offsetHeight;
                /** @type {?} */
                var scrollPercent = scrollWidth / totalWidth;
                /** @type {?} */
                var scrollHeight = scrollPercent * totalHeight;
                this.transform = "translate3d(0px, " + (-this.activeIndex * totalHeight + scrollHeight) + "px, 0px)";
            }
            else {
                this.transform = "translate3d(" + (-this.activeIndex * totalWidth + scrollWidth) + "px, 0px, 0px)";
            }
            if (this.slickTrack) {
                this.renderer.setStyle(this.slickTrack.nativeElement, 'transform', this.transform);
            }
        }
        if (e.isFinal) {
            this.setUpNextScroll();
        }
        else {
            this.clearTimeout();
        }
    };
    /**
     * @return {?}
     */
    NzCarouselComponent.prototype.clearTimeout = /**
     * @return {?}
     */
    function () {
        if (this.transitionAction) {
            clearTimeout(this.transitionAction);
            this.transitionAction = null;
        }
    };
    /**
     * Make a carousel scroll to `this.nextIndex` after `this.nzAutoPlaySpeed` milliseconds.
     */
    /**
     * Make a carousel scroll to `this.nextIndex` after `this.nzAutoPlaySpeed` milliseconds.
     * @private
     * @return {?}
     */
    NzCarouselComponent.prototype.setUpNextScroll = /**
     * Make a carousel scroll to `this.nextIndex` after `this.nzAutoPlaySpeed` milliseconds.
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        this.clearTimeout();
        if (this.nzAutoPlay && this.nzAutoPlaySpeed > 0) {
            this.transitionAction = setTimeout((/**
             * @return {?}
             */
            function () {
                _this.setContentActive(_this.nextIndex);
            }), this.nzAutoPlaySpeed);
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCarouselComponent.prototype.updateMode = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.slideContents && this.slideContents.length) {
            this.renderContent();
            this.setContentActive(0);
        }
    };
    /**
     * @private
     * @return {?}
     */
    NzCarouselComponent.prototype.renderContent = /**
     * @private
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var slickTrackElement = this.slickTrack.nativeElement;
        /** @type {?} */
        var slickListElement = this.slickList.nativeElement;
        if (this.slideContents && this.slideContents.length) {
            this.slideContents.forEach((/**
             * @param {?} content
             * @param {?} i
             * @return {?}
             */
            function (content, i) {
                content.width = _this.el.offsetWidth;
                if (_this.nzEffect === 'fade') {
                    content.fadeMode = true;
                    if (_this.nzVertical) {
                        content.top = -i * _this.el.offsetHeight;
                    }
                    else {
                        content.left = -i * content.width;
                    }
                }
                else {
                    content.fadeMode = false;
                    content.left = null;
                    content.top = null;
                }
            }));
            if (this.nzVertical) {
                this.renderer.removeStyle(slickTrackElement, 'width');
                this.renderer.removeStyle(slickListElement, 'width');
                this.renderer.setStyle(slickListElement, 'height', this.slideContents.first.el.offsetHeight + "px");
                this.renderer.setStyle(slickTrackElement, 'height', this.slideContents.length * this.el.offsetHeight + "px");
            }
            else {
                this.renderer.removeStyle(slickTrackElement, 'height');
                this.renderer.removeStyle(slickListElement, 'height');
                this.renderer.removeStyle(slickTrackElement, 'width'); // This is necessary to prevent carousel items to overflow.
                this.renderer.setStyle(slickTrackElement, 'width', this.slideContents.length * this.el.offsetWidth + "px");
            }
            this.setUpNextScroll();
            this.cdr.markForCheck();
        }
    };
    NzCarouselComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-carousel',
                    preserveWhitespaces: false,
                    template: "<div class=\"slick-initialized slick-slider\" [class.slick-vertical]=\"nzVertical\">\n  <div\n    class=\"slick-list\"\n    #slickList\n    tabindex=\"-1\"\n    (keydown)=\"onKeyDown($event)\"\n    (swipeleft)=\"swipe('swipeleft')\"\n    (swiperight)=\"swipe('swiperight')\"\n    (pan)=\"swipeInProgress($event);\">\n    <div class=\"slick-track\" #slickTrack>\n      <ng-content></ng-content>\n    </div>\n  </div>\n  <ul class=\"slick-dots\" *ngIf=\"nzDots\">\n    <li *ngFor=\"let content of slideContents; let i = index\" [class.slick-active]=\"content.isActive\" (click)=\"goTo(i)\">\n      <ng-template [ngTemplateOutlet]=\"nzDotRender || renderDotTemplate\" [ngTemplateOutletContext]=\"{ $implicit: i }\"></ng-template>\n    </li>\n  </ul>\n</div>\n\n<ng-template #renderDotTemplate let-index>\n  <button>{{index + 1}}</button>\n</ng-template>\n",
                    host: {
                        '[class.ant-carousel-vertical]': 'nzVertical'
                    },
                    styles: ["\n      nz-carousel {\n        display: block;\n        position: relative;\n        overflow: hidden;\n        width: 100%;\n        height: 100%;\n      }\n\n      .slick-dots {\n        display: block;\n      }\n\n      .slick-track {\n        opacity: 1;\n        transition: all 0.5s ease;\n      }\n\n      .slick-slide {\n        transition: opacity 500ms ease;\n      }\n    "]
                }] }
    ];
    /** @nocollapse */
    NzCarouselComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: Renderer2 },
        { type: ChangeDetectorRef },
        { type: NgZone }
    ]; };
    NzCarouselComponent.propDecorators = {
        slideContents: [{ type: ContentChildren, args: [NzCarouselContentDirective,] }],
        slickList: [{ type: ViewChild, args: ['slickList',] }],
        slickTrack: [{ type: ViewChild, args: ['slickTrack',] }],
        nzTransitionSpeed: [{ type: Input }],
        nzDotRender: [{ type: Input }],
        nzEffect: [{ type: Input }],
        nzEnableSwipe: [{ type: Input }],
        nzDots: [{ type: Input }],
        nzVertical: [{ type: Input }],
        nzAutoPlay: [{ type: Input }],
        nzAutoPlaySpeed: [{ type: Input }],
        nzAfterChange: [{ type: Output }],
        nzBeforeChange: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCarouselComponent.prototype, "nzEnableSwipe", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzCarouselComponent.prototype, "nzDots", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzCarouselComponent.prototype, "nzVertical", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzCarouselComponent.prototype, "nzAutoPlay", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Object)
    ], NzCarouselComponent.prototype, "nzAutoPlaySpeed", void 0);
    return NzCarouselComponent;
}());
export { NzCarouselComponent };
if (false) {
    /** @type {?} */
    NzCarouselComponent.prototype.slideContents;
    /** @type {?} */
    NzCarouselComponent.prototype.slickList;
    /** @type {?} */
    NzCarouselComponent.prototype.slickTrack;
    /** @type {?} */
    NzCarouselComponent.prototype.nzTransitionSpeed;
    /** @type {?} */
    NzCarouselComponent.prototype.nzDotRender;
    /** @type {?} */
    NzCarouselComponent.prototype.nzEffect;
    /** @type {?} */
    NzCarouselComponent.prototype.nzEnableSwipe;
    /** @type {?} */
    NzCarouselComponent.prototype.nzDots;
    /** @type {?} */
    NzCarouselComponent.prototype.nzVertical;
    /** @type {?} */
    NzCarouselComponent.prototype.nzAutoPlay;
    /** @type {?} */
    NzCarouselComponent.prototype.nzAutoPlaySpeed;
    /** @type {?} */
    NzCarouselComponent.prototype.nzAfterChange;
    /** @type {?} */
    NzCarouselComponent.prototype.nzBeforeChange;
    /** @type {?} */
    NzCarouselComponent.prototype.activeIndex;
    /** @type {?} */
    NzCarouselComponent.prototype.transform;
    /** @type {?} */
    NzCarouselComponent.prototype.transitionAction;
    /**
     * @type {?}
     * @private
     */
    NzCarouselComponent.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzCarouselComponent.prototype.subs_;
    /** @type {?} */
    NzCarouselComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzCarouselComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzCarouselComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzCarouselComponent.prototype.ngZone;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotY2Fyb3VzZWwuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbImNhcm91c2VsL256LWNhcm91c2VsLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxVQUFVLEVBQUUsV0FBVyxFQUFFLE1BQU0sdUJBQXVCLENBQUM7QUFDaEUsT0FBTyxFQUdMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULGVBQWUsRUFDZixVQUFVLEVBQ1YsWUFBWSxFQUNaLEtBQUssRUFDTCxNQUFNLEVBR04sTUFBTSxFQUNOLFNBQVMsRUFDVCxTQUFTLEVBRVQsV0FBVyxFQUNYLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFDL0MsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQzlDLE9BQU8sRUFBRSxZQUFZLEVBQUUsV0FBVyxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDakUsT0FBTyxFQUFFLDBCQUEwQixFQUFFLE1BQU0saUNBQWlDLENBQUM7QUFNN0U7SUFrRUUsNkJBQ1MsVUFBc0IsRUFDckIsUUFBbUIsRUFDbkIsR0FBc0IsRUFDdEIsTUFBYztRQUhmLGVBQVUsR0FBVixVQUFVLENBQVk7UUFDckIsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQUNuQixRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQUN0QixXQUFNLEdBQU4sTUFBTSxDQUFRO1FBL0JmLHNCQUFpQixHQUFHLEdBQUcsQ0FBQyxDQUFDLGVBQWU7UUFFeEMsYUFBUSxHQUFzQixTQUFTLENBQUM7UUFDeEIsa0JBQWEsR0FBRyxJQUFJLENBQUM7UUFDckIsV0FBTSxHQUFZLElBQUksQ0FBQztRQUN2QixlQUFVLEdBQVksS0FBSyxDQUFDO1FBQzVCLGVBQVUs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TUFBTTtpQ0FDTixNQUFNOztJQVBrQjtRQUFmLFlBQVksRUFBRTs7OERBQXNCO0lBQ3JCO1FBQWYsWUFBWSxFQUFFOzt1REFBd0I7SUFDdkI7UUFBZixZQUFZLEVBQUU7OzJEQUE2QjtJQUM1QjtRQUFmLFlBQVksRUFBRTs7MkRBQW9CO0lBQ3BCO1FBQWQsV0FBVyxFQUFFOztnRUFBd0I7SUFnT2pELDBCQUFDO0NBQUEsQUE5UUQsSUE4UUM7U0E1T1ksbUJBQW1COzs7SUFDOUIsNENBQWtHOztJQUNsRyx3Q0FBOEM7O0lBQzlDLHlDQUFnRDs7SUFFaEQsZ0RBQWlDOztJQUNqQywwQ0FBeUQ7O0lBQ3pELHVDQUFpRDs7SUFDakQsNENBQThDOztJQUM5QyxxQ0FBZ0Q7O0lBQ2hELHlDQUFxRDs7SUFDckQseUNBQTRDOztJQUM1Qyw4Q0FBK0M7O0lBRS9DLDRDQUE0RTs7SUFDNUUsNkNBQW1HOztJQUVuRywwQ0FBZ0I7O0lBQ2hCLHdDQUF5Qzs7SUFDekMsK0NBQWdDOzs7OztJQUVoQyxpQ0FBMkM7Ozs7O0lBQzNDLG9DQUFtQzs7SUFXakMseUNBQTZCOzs7OztJQUM3Qix1Q0FBMkI7Ozs7O0lBQzNCLGtDQUE4Qjs7Ozs7SUFDOUIscUNBQXNCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgTEVGVF9BUlJPVywgUklHSFRfQVJST1cgfSBmcm9tICdAYW5ndWxhci9jZGsva2V5Y29kZXMnO1xuaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZHJlbixcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgTmdab25lLFxuICBPbkNoYW5nZXMsXG4gIE9uRGVzdHJveSxcbiAgT3V0cHV0LFxuICBRdWVyeUxpc3QsXG4gIFJlbmRlcmVyMixcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBmcm9tRXZlbnQsIFN1YnNjcmlwdGlvbiB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgZGVib3VuY2VUaW1lIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuLCBJbnB1dE51bWJlciB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56Q2Fyb3VzZWxDb250ZW50RGlyZWN0aXZlIH0gZnJvbSAnLi9uei1jYXJvdXNlbC1jb250ZW50LmRpcmVjdGl2ZSc7XG5cbmV4cG9ydCB0eXBlIE56Q2Fyb3VzZWxFZmZlY3RzID0gJ2ZhZGUnIHwgJ3Njcm9sbHgnO1xuXG5leHBvcnQgdHlwZSBTd2lwZURpcmVjdGlvbiA9ICdzd2lwZWxlZnQnIHwgJ3N3aXBlcmlnaHQnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotY2Fyb3VzZWwnLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWNhcm91c2VsLmNvbXBvbmVudC5odG1sJyxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LWNhcm91c2VsLXZlcnRpY2FsXSc6ICduelZlcnRpY2FsJ1xuICB9LFxuICBzdHlsZXM6IFtcbiAgICBgXG4gICAgICBuei1jYXJvdXNlbCB7XG4gICAgICAgIGRpc3BsYXk6IGJsb2NrO1xuICAgICAgICBwb3NpdGlvbjogcmVsYXRpdmU7XG4gICAgICAgIG92ZXJmbG93OiBoaWRkZW47XG4gICAgICAgIHdpZHRoOiAxMDAlO1xuICAgICAgICBoZWlnaHQ6IDEwMCU7XG4gICAgICB9XG5cbiAgICAgIC5zbGljay1kb3RzIHtcbiAgICAgICAgZGlzcGxheTogYmxvY2s7XG4gICAgICB9XG5cbiAgICAgIC5zbGljay10cmFjayB7XG4gICAgICAgIG9wYWNpdHk6IDE7XG4gICAgICAgIHRyYW5zaXRpb246IGFsbCAwLjVzIGVhc2U7XG4gICAgICB9XG5cbiAgICAgIC5zbGljay1zbGlkZSB7XG4gICAgICAgIHRyYW5zaXRpb246IG9wYWNpdHkgNTAwbXMgZWFzZTtcbiAgICAgIH1cbiAgICBgXG4gIF1cbn0pXG5leHBvcnQgY2xhc3MgTnpDYXJvdXNlbENvbXBvbmVudCBpbXBsZW1lbnRzIEFmdGVyVmlld0luaXQsIEFmdGVyQ29udGVudEluaXQsIE9uRGVzdHJveSwgT25DaGFuZ2VzIHtcbiAgQENvbnRlbnRDaGlsZHJlbihOekNhcm91c2VsQ29udGVudERpcmVjdGl2ZSkgc2xpZGVDb250ZW50czogUXVlcnlMaXN0PE56Q2Fyb3VzZWxDb250ZW50RGlyZWN0aXZlPjtcbiAgQFZpZXdDaGlsZCgnc2xpY2tMaXN0Jykgc2xpY2tMaXN0OiBFbGVtZW50UmVmO1xuICBAVmlld0NoaWxkKCdzbGlja1RyYWNrJykgc2xpY2tUcmFjazogRWxlbWVudFJlZjtcblxuICBASW5wdXQoKSBuelRyYW5zaXRpb25TcGVlZCA9IDUwMDsgLy8gTm90IGV4cG9zZWQuXG4gIEBJbnB1dCgpIG56RG90UmVuZGVyOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogbnVtYmVyIH0+O1xuICBASW5wdXQoKSBuekVmZmVjdDogTnpDYXJvdXNlbEVmZmVjdHMgPSAnc2Nyb2xseCc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekVuYWJsZVN3aXBlID0gdHJ1ZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56RG90czogYm9vbGVhbiA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuelZlcnRpY2FsOiBib29sZWFuID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekF1dG9QbGF5ID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56QXV0b1BsYXlTcGVlZCA9IDMwMDA7IC8vIFNob3VsZCBiZSBuekF1dG9QbGF5RHVyYXRpb24sIGJ1dCBjaGFuZ2luZyB0aGlzIGlzIGJyZWFraW5nLlxuXG4gIEBPdXRwdXQoKSByZWFkb25seSBuekFmdGVyQ2hhbmdlOiBFdmVudEVtaXR0ZXI8bnVtYmVyPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56QmVmb3JlQ2hhbmdlOiBFdmVudEVtaXR0ZXI8eyBmcm9tOiBudW1iZXI7IHRvOiBudW1iZXIgfT4gPSBuZXcgRXZlbnRFbWl0dGVyKCk7XG5cbiAgYWN0aXZlSW5kZXggPSAwO1xuICB0cmFuc2Zvcm0gPSAndHJhbnNsYXRlM2QoMHB4LCAwcHgsIDBweCknO1xuICB0cmFuc2l0aW9uQWN0aW9uOiBudW1iZXIgfCBudWxsO1xuXG4gIHByaXZhdGUgZWwgPSB0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudDtcbiAgcHJpdmF0ZSBzdWJzXyA9IG5ldyBTdWJzY3JpcHRpb24oKTtcblxuICBnZXQgbmV4dEluZGV4KCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuYWN0aXZlSW5kZXggPCB0aGlzLnNsaWRlQ29udGVudHMubGVuZ3RoIC0gMSA/IHRoaXMuYWN0aXZlSW5kZXggKyAxIDogMDtcbiAgfVxuXG4gIGdldCBwcmV2SW5kZXgoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5hY3RpdmVJbmRleCA+IDAgPyB0aGlzLmFjdGl2ZUluZGV4IC0gMSA6IHRoaXMuc2xpZGVDb250ZW50cy5sZW5ndGggLSAxO1xuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHVibGljIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyLFxuICAgIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBwcml2YXRlIG5nWm9uZTogTmdab25lXG4gICkge1xuICAgIHJlbmRlcmVyLmFkZENsYXNzKGVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2FudC1jYXJvdXNlbCcpO1xuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnNsaWRlQ29udGVudHMgJiYgdGhpcy5zbGlkZUNvbnRlbnRzLmxlbmd0aCkge1xuICAgICAgdGhpcy5zbGlkZUNvbnRlbnRzLmZpcnN0LmlzQWN0aXZlID0gdHJ1ZTtcbiAgICB9XG4gIH1cblxuICBuZ0FmdGVyVmlld0luaXQoKTogdm9pZCB7XG4gICAgLy8gUmUtcmVuZGVyIHdoZW4gY29udGVudCBjaGFuZ2VzLlxuICAgIHRoaXMuc3Vic18uYWRkKFxuICAgICAgdGhpcy5zbGlkZUNvbnRlbnRzLmNoYW5nZXMuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgICAgdGhpcy5yZW5kZXJDb250ZW50KCk7XG4gICAgICB9KVxuICAgICk7XG5cbiAgICB0aGlzLm5nWm9uZS5ydW5PdXRzaWRlQW5ndWxhcigoKSA9PiB7XG4gICAgICB0aGlzLnN1YnNfLmFkZChcbiAgICAgICAgZnJvbUV2ZW50KHdpbmRvdywgJ3Jlc2l6ZScpXG4gICAgICAgICAgLnBpcGUoZGVib3VuY2VUaW1lKDUwKSlcbiAgICAgICAgICAuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgICAgICAgIHRoaXMucmVuZGVyQ29udGVudCgpO1xuICAgICAgICAgICAgdGhpcy5zZXRUcmFuc2l0aW9uKCk7XG4gICAgICAgICAgfSlcbiAgICAgICk7XG4gICAgfSk7XG5cbiAgICAvLyBXaGVuIHVzZWQgaW4gbW9kYWxzIChkcmF3ZXJzIG1heWJlIHRvbyksIGl0IHNob3VsZCByZW5kZXIgaXRzZWxmIGFzeW5jaHJvbm91c2x5LlxuICAgIC8vIFJlZmVyIHRvIGh0dHBzOi8vZ2l0aHViLmNvbS9ORy1aT1JSTy9uZy16b3Jyby1hbnRkL2lzc3Vlcy8yMzg3XG4gICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB7XG4gICAgICB0aGlzLnJlbmRlckNvbnRlbnQoKTtcbiAgICB9KTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uekF1dG9QbGF5IHx8IGNoYW5nZXMubnpBdXRvUGxheVNwZWVkKSB7XG4gICAgICB0aGlzLnNldFVwTmV4dFNjcm9sbCgpO1xuICAgIH1cbiAgICBpZiAoY2hhbmdlcy5uekVmZmVjdCkge1xuICAgICAgdGhpcy51cGRhdGVNb2RlKCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5zdWJzXy51bnN1YnNjcmliZSgpO1xuICAgIHRoaXMuY2xlYXJUaW1lb3V0KCk7XG4gIH1cblxuICBzZXRDb250ZW50QWN0aXZlKGluZGV4OiBudW1iZXIpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5zbGlkZUNvbnRlbnRzICYmIHRoaXMuc2xpZGVDb250ZW50cy5sZW5ndGgpIHtcbiAgICAgIHRoaXMubnpCZWZvcmVDaGFuZ2UuZW1pdCh7IGZyb206IHRoaXMuc2xpZGVDb250ZW50cy50b0FycmF5KCkuZmluZEluZGV4KHNsaWRlID0+IHNsaWRlLmlzQWN0aXZlKSwgdG86IGluZGV4IH0pO1xuICAgICAgdGhpcy5hY3RpdmVJbmRleCA9IGluZGV4O1xuICAgICAgdGhpcy5zZXRUcmFuc2l0aW9uKCk7XG4gICAgICB0aGlzLnNsaWRlQ29udGVudHMuZm9yRWFjaCgoc2xpZGUsIGkpID0+IChzbGlkZS5pc0FjdGl2ZSA9IGluZGV4ID09PSBpKSk7XG4gICAgICB0aGlzLnNldFVwTmV4dFNjcm9sbCgpO1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgICAvLyBTaG91bGQgdHJpZ2dlciB0aGUgZm9sbG93aW5nIHdoZW4gYW5pbWF0aW9uIGlzIGRvbmUuIFRoZSB0cmFuc2l0aW9uIHRha2VzIDAuNSBzZWNvbmRzIGFjY29yZGluZyB0byB0aGUgQ1NTLlxuICAgICAgc2V0VGltZW91dCgoKSA9PiB0aGlzLm56QWZ0ZXJDaGFuZ2UuZW1pdChpbmRleCksIHRoaXMubnpUcmFuc2l0aW9uU3BlZWQpO1xuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgc2V0VHJhbnNpdGlvbigpOiB2b2lkIHtcbiAgICB0aGlzLnRyYW5zZm9ybSA9XG4gICAgICB0aGlzLm56RWZmZWN0ID09PSAnZmFkZSdcbiAgICAgICAgPyAndHJhbnNsYXRlM2QoMHB4LCAwcHgsIDBweCknXG4gICAgICAgIDogdGhpcy5uelZlcnRpY2FsXG4gICAgICAgID8gLy8gYFNjcm9sbHhgIG1vZGUuXG4gICAgICAgICAgYHRyYW5zbGF0ZTNkKDBweCwgJHstdGhpcy5hY3RpdmVJbmRleCAqIHRoaXMuZWwub2Zmc2V0SGVpZ2h0fXB4LCAwcHgpYFxuICAgICAgICA6IGB0cmFuc2xhdGUzZCgkey10aGlzLmFjdGl2ZUluZGV4ICogdGhpcy5lbC5vZmZzZXRXaWR0aH1weCwgMHB4LCAwcHgpYDtcbiAgICBpZiAodGhpcy5zbGlja1RyYWNrKSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnNldFN0eWxlKHRoaXMuc2xpY2tUcmFjay5uYXRpdmVFbGVtZW50LCAndHJhbnNmb3JtJywgdGhpcy50cmFuc2Zvcm0pO1xuICAgIH1cbiAgfVxuXG4gIG5leHQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDb250ZW50QWN0aXZlKHRoaXMubmV4dEluZGV4KTtcbiAgfVxuXG4gIHByZSgpOiB2b2lkIHtcbiAgICB0aGlzLnNldENvbnRlbnRBY3RpdmUodGhpcy5wcmV2SW5kZXgpO1xuICB9XG5cbiAgZ29UbyhpbmRleDogbnVtYmVyKTogdm9pZCB7XG4gICAgaWYgKGluZGV4ID49IDAgJiYgaW5kZXggPD0gdGhpcy5zbGlkZUNvbnRlbnRzLmxlbmd0aCAtIDEpIHtcbiAgICAgIHRoaXMuc2V0Q29udGVudEFjdGl2ZShpbmRleCk7XG4gICAgfVxuICB9XG5cbiAgb25LZXlEb3duKGU6IEtleWJvYXJkRXZlbnQpOiB2b2lkIHtcbiAgICBpZiAoZS5rZXlDb2RlID09PSBMRUZUX0FSUk9XKSB7XG4gICAgICAvLyBMZWZ0XG4gICAgICB0aGlzLnByZSgpO1xuICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIH0gZWxzZSBpZiAoZS5rZXlDb2RlID09PSBSSUdIVF9BUlJPVykge1xuICAgICAgLy8gUmlnaHRcbiAgICAgIHRoaXMubmV4dCgpO1xuICAgICAgZS5wcmV2ZW50RGVmYXVsdCgpO1xuICAgIH1cbiAgfVxuXG4gIHN3aXBlKGFjdGlvbjogU3dpcGVEaXJlY3Rpb24gPSAnc3dpcGVsZWZ0Jyk6IHZvaWQge1xuICAgIGlmICghdGhpcy5uekVuYWJsZVN3aXBlKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIGlmIChhY3Rpb24gPT09ICdzd2lwZWxlZnQnKSB7XG4gICAgICB0aGlzLm5leHQoKTtcbiAgICB9XG4gICAgaWYgKGFjdGlvbiA9PT0gJ3N3aXBlcmlnaHQnKSB7XG4gICAgICB0aGlzLnByZSgpO1xuICAgIH1cbiAgfVxuXG4gIC8qIHRzbGludDpkaXNhYmxlLW5leHQtbGluZTpuby1hbnkgKi9cbiAgc3dpcGVJblByb2dyZXNzKGU6IGFueSk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56RWZmZWN0ID09PSAnc2Nyb2xseCcpIHtcbiAgICAgIGNvbnN0IGZpbmFsID0gZS5pc0ZpbmFsO1xuICAgICAgY29uc3Qgc2Nyb2xsV2lkdGggPSBmaW5hbCA/IDAgOiBlLmRlbHRhWCAqIDEuMjtcbiAgICAgIGNvbnN0IHRvdGFsV2lkdGggPSB0aGlzLmVsLm9mZnNldFdpZHRoO1xuICAgICAgaWYgKHRoaXMubnpWZXJ0aWNhbCkge1xuICAgICAgICBjb25zdCB0b3RhbEhlaWdodCA9IHRoaXMuZWwub2Zmc2V0SGVpZ2h0O1xuICAgICAgICBjb25zdCBzY3JvbGxQZXJjZW50ID0gc2Nyb2xsV2lkdGggLyB0b3RhbFdpZHRoO1xuICAgICAgICBjb25zdCBzY3JvbGxIZWlnaHQgPSBzY3JvbGxQZXJjZW50ICogdG90YWxIZWlnaHQ7XG4gICAgICAgIHRoaXMudHJhbnNmb3JtID0gYHRyYW5zbGF0ZTNkKDBweCwgJHstdGhpcy5hY3RpdmVJbmRleCAqIHRvdGFsSGVpZ2h0ICsgc2Nyb2xsSGVpZ2h0fXB4LCAwcHgpYDtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMudHJhbnNmb3JtID0gYHRyYW5zbGF0ZTNkKCR7LXRoaXMuYWN0aXZlSW5kZXggKiB0b3RhbFdpZHRoICsgc2Nyb2xsV2lkdGh9cHgsIDBweCwgMHB4KWA7XG4gICAgICB9XG4gICAgICBpZiAodGhpcy5zbGlja1RyYWNrKSB7XG4gICAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5zbGlja1RyYWNrLm5hdGl2ZUVsZW1lbnQsICd0cmFuc2Zvcm0nLCB0aGlzLnRyYW5zZm9ybSk7XG4gICAgICB9XG4gICAgfVxuICAgIGlmIChlLmlzRmluYWwpIHtcbiAgICAgIHRoaXMuc2V0VXBOZXh0U2Nyb2xsKCk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMuY2xlYXJUaW1lb3V0KCk7XG4gICAgfVxuICB9XG5cbiAgY2xlYXJUaW1lb3V0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnRyYW5zaXRpb25BY3Rpb24pIHtcbiAgICAgIGNsZWFyVGltZW91dCh0aGlzLnRyYW5zaXRpb25BY3Rpb24pO1xuICAgICAgdGhpcy50cmFuc2l0aW9uQWN0aW9uID0gbnVsbDtcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogTWFrZSBhIGNhcm91c2VsIHNjcm9sbCB0byBgdGhpcy5uZXh0SW5kZXhgIGFmdGVyIGB0aGlzLm56QXV0b1BsYXlTcGVlZGAgbWlsbGlzZWNvbmRzLlxuICAgKi9cbiAgcHJpdmF0ZSBzZXRVcE5leHRTY3JvbGwoKTogdm9pZCB7XG4gICAgdGhpcy5jbGVhclRpbWVvdXQoKTtcbiAgICBpZiAodGhpcy5uekF1dG9QbGF5ICYmIHRoaXMubnpBdXRvUGxheVNwZWVkID4gMCkge1xuICAgICAgdGhpcy50cmFuc2l0aW9uQWN0aW9uID0gc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIHRoaXMuc2V0Q29udGVudEFjdGl2ZSh0aGlzLm5leHRJbmRleCk7XG4gICAgICB9LCB0aGlzLm56QXV0b1BsYXlTcGVlZCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSB1cGRhdGVNb2RlKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnNsaWRlQ29udGVudHMgJiYgdGhpcy5zbGlkZUNvbnRlbnRzLmxlbmd0aCkge1xuICAgICAgdGhpcy5yZW5kZXJDb250ZW50KCk7XG4gICAgICB0aGlzLnNldENvbnRlbnRBY3RpdmUoMCk7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSByZW5kZXJDb250ZW50KCk6IHZvaWQge1xuICAgIGNvbnN0IHNsaWNrVHJhY2tFbGVtZW50ID0gdGhpcy5zbGlja1RyYWNrLm5hdGl2ZUVsZW1lbnQ7XG4gICAgY29uc3Qgc2xpY2tMaXN0RWxlbWVudCA9IHRoaXMuc2xpY2tMaXN0Lm5hdGl2ZUVsZW1lbnQ7XG4gICAgaWYgKHRoaXMuc2xpZGVDb250ZW50cyAmJiB0aGlzLnNsaWRlQ29udGVudHMubGVuZ3RoKSB7XG4gICAgICB0aGlzLnNsaWRlQ29udGVudHMuZm9yRWFjaCgoY29udGVudCwgaSkgPT4ge1xuICAgICAgICBjb250ZW50LndpZHRoID0gdGhpcy5lbC5vZmZzZXRXaWR0aDtcbiAgICAgICAgaWYgKHRoaXMubnpFZmZlY3QgPT09ICdmYWRlJykge1xuICAgICAgICAgIGNvbnRlbnQuZmFkZU1vZGUgPSB0cnVlO1xuICAgICAgICAgIGlmICh0aGlzLm56VmVydGljYWwpIHtcbiAgICAgICAgICAgIGNvbnRlbnQudG9wID0gLWkgKiB0aGlzLmVsLm9mZnNldEhlaWdodDtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgY29udGVudC5sZWZ0ID0gLWkgKiBjb250ZW50LndpZHRoO1xuICAgICAgICAgIH1cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBjb250ZW50LmZhZGVNb2RlID0gZmFsc2U7XG4gICAgICAgICAgY29udGVudC5sZWZ0ID0gbnVsbDtcbiAgICAgICAgICBjb250ZW50LnRvcCA9IG51bGw7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgaWYgKHRoaXMubnpWZXJ0aWNhbCkge1xuICAgICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZVN0eWxlKHNsaWNrVHJhY2tFbGVtZW50LCAnd2lkdGgnKTtcbiAgICAgICAgdGhpcy5yZW5kZXJlci5yZW1vdmVTdHlsZShzbGlja0xpc3RFbGVtZW50LCAnd2lkdGgnKTtcbiAgICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZShzbGlja0xpc3RFbGVtZW50LCAnaGVpZ2h0JywgYCR7dGhpcy5zbGlkZUNvbnRlbnRzLmZpcnN0LmVsLm9mZnNldEhlaWdodH1weGApO1xuICAgICAgICB0aGlzLnJlbmRlcmVyLnNldFN0eWxlKHNsaWNrVHJhY2tFbGVtZW50LCAnaGVpZ2h0JywgYCR7dGhpcy5zbGlkZUNvbnRlbnRzLmxlbmd0aCAqIHRoaXMuZWwub2Zmc2V0SGVpZ2h0fXB4YCk7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZVN0eWxlKHNsaWNrVHJhY2tFbGVtZW50LCAnaGVpZ2h0Jyk7XG4gICAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlU3R5bGUoc2xpY2tMaXN0RWxlbWVudCwgJ2hlaWdodCcpO1xuICAgICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZVN0eWxlKHNsaWNrVHJhY2tFbGVtZW50LCAnd2lkdGgnKTsgLy8gVGhpcyBpcyBuZWNlc3NhcnkgdG8gcHJldmVudCBjYXJvdXNlbCBpdGVtcyB0byBvdmVyZmxvdy5cbiAgICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZShzbGlja1RyYWNrRWxlbWVudCwgJ3dpZHRoJywgYCR7dGhpcy5zbGlkZUNvbnRlbnRzLmxlbmd0aCAqIHRoaXMuZWwub2Zmc2V0V2lkdGh9cHhgKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuc2V0VXBOZXh0U2Nyb2xsKCk7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==