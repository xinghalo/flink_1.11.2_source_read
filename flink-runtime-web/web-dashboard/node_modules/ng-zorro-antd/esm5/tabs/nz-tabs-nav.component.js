/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
/** code from https://github.com/angular/material2 */
import { Directionality } from '@angular/cdk/bidi';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChildren, ElementRef, EventEmitter, Input, NgZone, Optional, Output, QueryList, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { fromEvent, merge, of as observableOf } from 'rxjs';
import { auditTime, startWith } from 'rxjs/operators';
import { InputBoolean } from '../core/util/convert';
import { NzTabLabelDirective } from './nz-tab-label.directive';
import { NzTabsInkBarDirective } from './nz-tabs-ink-bar.directive';
/** @type {?} */
var EXAGGERATED_OVERSCROLL = 64;
var NzTabsNavComponent = /** @class */ (function () {
    function NzTabsNavComponent(elementRef, ngZone, renderer, cdr, dir) {
        this.elementRef = elementRef;
        this.ngZone = ngZone;
        this.renderer = renderer;
        this.cdr = cdr;
        this.dir = dir;
        this._tabPositionMode = 'horizontal';
        this._scrollDistance = 0;
        this._selectedIndex = 0;
        this.showPaginationControls = false;
        this.disableScrollAfter = true;
        this.disableScrollBefore = true;
        this.selectedIndexChanged = false;
        this.realignInkBar = null;
        this.nzOnNextClick = new EventEmitter();
        this.nzOnPrevClick = new EventEmitter();
        this.nzAnimated = true;
        this.nzHideBar = false;
        this.nzShowPagination = true;
        this.nzType = 'line';
    }
    Object.defineProperty(NzTabsNavComponent.prototype, "nzPositionMode", {
        get: /**
         * @return {?}
         */
        function () {
            return this._tabPositionMode;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            var _this = this;
            this._tabPositionMode = value;
            this.alignInkBarToSelectedTab();
            if (this.nzShowPagination) {
                Promise.resolve().then((/**
                 * @return {?}
                 */
                function () {
                    _this.updatePagination();
                }));
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTabsNavComponent.prototype, "selectedIndex", {
        get: /**
         * @return {?}
         */
        function () {
            return this._selectedIndex;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            this.selectedIndexChanged = this._selectedIndex !== value;
            this._selectedIndex = value;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.onContentChanges = /**
     * @return {?}
     */
    function () {
        var _this = this;
        /** @type {?} */
        var textContent = this.elementRef.nativeElement.textContent;
        // We need to diff the text content of the header, because the MutationObserver callback
        // will fire even if the text content didn't change which is inefficient and is prone
        // to infinite loops if a poorly constructed expression is passed in (see #14249).
        if (textContent !== this.currentTextContent) {
            this.ngZone.run((/**
             * @return {?}
             */
            function () {
                if (_this.nzShowPagination) {
                    _this.updatePagination();
                }
                _this.alignInkBarToSelectedTab();
                _this.cdr.markForCheck();
            }));
        }
    };
    /**
     * @param {?} scrollDir
     * @return {?}
     */
    NzTabsNavComponent.prototype.scrollHeader = /**
     * @param {?} scrollDir
     * @return {?}
     */
    function (scrollDir) {
        if (scrollDir === 'before' && !this.disableScrollBefore) {
            this.nzOnPrevClick.emit();
        }
        else if (scrollDir === 'after' && !this.disableScrollAfter) {
            this.nzOnNextClick.emit();
        }
        // Move the scroll distance one-third the length of the tab list's viewport.
        this.scrollDistance += ((scrollDir === 'before' ? -1 : 1) * this.viewWidthHeightPix) / 3;
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.ngAfterContentChecked = /**
     * @return {?}
     */
    function () {
        if (this.tabLabelCount !== this.listOfNzTabLabelDirective.length) {
            if (this.nzShowPagination) {
                this.updatePagination();
            }
            this.tabLabelCount = this.listOfNzTabLabelDirective.length;
            this.cdr.markForCheck();
        }
        if (this.selectedIndexChanged) {
            this.scrollToLabel(this._selectedIndex);
            if (this.nzShowPagination) {
                this.checkScrollingControls();
            }
            this.alignInkBarToSelectedTab();
            this.selectedIndexChanged = false;
            this.cdr.markForCheck();
        }
        if (this.scrollDistanceChanged) {
            if (this.nzShowPagination) {
                this.updateTabScrollPosition();
            }
            this.scrollDistanceChanged = false;
            this.cdr.markForCheck();
        }
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        this.realignInkBar = this.ngZone.runOutsideAngular((/**
         * @return {?}
         */
        function () {
            /** @type {?} */
            var dirChange = _this.dir ? _this.dir.change : observableOf(null);
            /** @type {?} */
            var resize = typeof window !== 'undefined' ? fromEvent(window, 'resize').pipe(auditTime(10)) : observableOf(null);
            return merge(dirChange, resize)
                .pipe(startWith(null))
                .subscribe((/**
             * @return {?}
             */
            function () {
                if (_this.nzShowPagination) {
                    _this.updatePagination();
                }
                _this.alignInkBarToSelectedTab();
            }));
        }));
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.updateTabScrollPosition = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var scrollDistance = this.scrollDistance;
        if (this.nzPositionMode === 'horizontal') {
            /** @type {?} */
            var translateX = this.getLayoutDirection() === 'ltr' ? -scrollDistance : scrollDistance;
            this.renderer.setStyle(this.navListElement.nativeElement, 'transform', "translate3d(" + translateX + "px, 0, 0)");
        }
        else {
            this.renderer.setStyle(this.navListElement.nativeElement, 'transform', "translate3d(0," + -scrollDistance + "px, 0)");
        }
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.updatePagination = /**
     * @return {?}
     */
    function () {
        this.checkPaginationEnabled();
        this.checkScrollingControls();
        this.updateTabScrollPosition();
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.checkPaginationEnabled = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var isEnabled = this.tabListScrollWidthHeightPix > this.tabListScrollOffSetWidthHeight;
        if (!isEnabled) {
            this.scrollDistance = 0;
        }
        if (isEnabled !== this.showPaginationControls) {
            this.cdr.markForCheck();
        }
        this.showPaginationControls = isEnabled;
    };
    /**
     * @param {?} labelIndex
     * @return {?}
     */
    NzTabsNavComponent.prototype.scrollToLabel = /**
     * @param {?} labelIndex
     * @return {?}
     */
    function (labelIndex) {
        /** @type {?} */
        var selectedLabel = this.listOfNzTabLabelDirective ? this.listOfNzTabLabelDirective.toArray()[labelIndex] : null;
        if (selectedLabel) {
            // The view length is the visible width of the tab labels.
            /** @type {?} */
            var labelBeforePos = void 0;
            /** @type {?} */
            var labelAfterPos = void 0;
            if (this.nzPositionMode === 'horizontal') {
                if (this.getLayoutDirection() === 'ltr') {
                    labelBeforePos = selectedLabel.getOffsetLeft();
                    labelAfterPos = labelBeforePos + selectedLabel.getOffsetWidth();
                }
                else {
                    labelAfterPos = this.navListElement.nativeElement.offsetWidth - selectedLabel.getOffsetLeft();
                    labelBeforePos = labelAfterPos - selectedLabel.getOffsetWidth();
                }
            }
            else {
                labelBeforePos = selectedLabel.getOffsetTop();
                labelAfterPos = labelBeforePos + selectedLabel.getOffsetHeight();
            }
            /** @type {?} */
            var beforeVisiblePos = this.scrollDistance;
            /** @type {?} */
            var afterVisiblePos = this.scrollDistance + this.viewWidthHeightPix;
            if (labelBeforePos < beforeVisiblePos) {
                // Scroll header to move label to the before direction
                this.scrollDistance -= beforeVisiblePos - labelBeforePos + EXAGGERATED_OVERSCROLL;
            }
            else if (labelAfterPos > afterVisiblePos) {
                // Scroll header to move label to the after direction
                this.scrollDistance += labelAfterPos - afterVisiblePos + EXAGGERATED_OVERSCROLL;
            }
        }
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.checkScrollingControls = /**
     * @return {?}
     */
    function () {
        // Check if the pagination arrows should be activated.
        this.disableScrollBefore = this.scrollDistance === 0;
        this.disableScrollAfter = this.scrollDistance === this.getMaxScrollDistance();
        this.cdr.markForCheck();
    };
    /**
     * Determines what is the maximum length in pixels that can be set for the scroll distance. This
     * is equal to the difference in width between the tab list container and tab header container.
     *
     * This is an expensive call that forces a layout reflow to compute box and scroll metrics and
     * should be called sparingly.
     */
    /**
     * Determines what is the maximum length in pixels that can be set for the scroll distance. This
     * is equal to the difference in width between the tab list container and tab header container.
     *
     * This is an expensive call that forces a layout reflow to compute box and scroll metrics and
     * should be called sparingly.
     * @return {?}
     */
    NzTabsNavComponent.prototype.getMaxScrollDistance = /**
     * Determines what is the maximum length in pixels that can be set for the scroll distance. This
     * is equal to the difference in width between the tab list container and tab header container.
     *
     * This is an expensive call that forces a layout reflow to compute box and scroll metrics and
     * should be called sparingly.
     * @return {?}
     */
    function () {
        return this.tabListScrollWidthHeightPix - this.viewWidthHeightPix || 0;
    };
    Object.defineProperty(NzTabsNavComponent.prototype, "scrollDistance", {
        get: /**
         * @return {?}
         */
        function () {
            return this._scrollDistance;
        },
        /** Sets the distance in pixels that the tab header should be transformed in the X-axis. */
        set: /**
         * Sets the distance in pixels that the tab header should be transformed in the X-axis.
         * @param {?} v
         * @return {?}
         */
        function (v) {
            this._scrollDistance = Math.max(0, Math.min(this.getMaxScrollDistance(), v));
            // Mark that the scroll distance has changed so that after the view is checked, the CSS
            // transformation can move the header.
            this.scrollDistanceChanged = true;
            this.checkScrollingControls();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTabsNavComponent.prototype, "viewWidthHeightPix", {
        get: /**
         * @return {?}
         */
        function () {
            /** @type {?} */
            var PAGINATION_PIX = 0;
            if (this.showPaginationControls) {
                PAGINATION_PIX = 64;
            }
            if (this.nzPositionMode === 'horizontal') {
                return this.navContainerElement.nativeElement.offsetWidth - PAGINATION_PIX;
            }
            else {
                return this.navContainerElement.nativeElement.offsetHeight - PAGINATION_PIX;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTabsNavComponent.prototype, "tabListScrollWidthHeightPix", {
        get: /**
         * @return {?}
         */
        function () {
            if (this.nzPositionMode === 'horizontal') {
                return this.navListElement.nativeElement.scrollWidth;
            }
            else {
                return this.navListElement.nativeElement.scrollHeight;
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzTabsNavComponent.prototype, "tabListScrollOffSetWidthHeight", {
        get: /**
         * @return {?}
         */
        function () {
            if (this.nzPositionMode === 'horizontal') {
                return this.scrollListElement.nativeElement.offsetWidth;
            }
            else {
                return this.elementRef.nativeElement.offsetHeight;
            }
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.getLayoutDirection = /**
     * @return {?}
     */
    function () {
        return this.dir && this.dir.value === 'rtl' ? 'rtl' : 'ltr';
    };
    /**
     * @return {?}
     */
    NzTabsNavComponent.prototype.alignInkBarToSelectedTab = /**
     * @return {?}
     */
    function () {
        if (this.nzType === 'line') {
            /** @type {?} */
            var selectedLabelWrapper = this.listOfNzTabLabelDirective && this.listOfNzTabLabelDirective.length
                ? this.listOfNzTabLabelDirective.toArray()[this.selectedIndex].elementRef.nativeElement
                : null;
            if (this.nzTabsInkBarDirective) {
                this.nzTabsInkBarDirective.alignToElement(selectedLabelWrapper);
            }
        }
    };
    NzTabsNavComponent.decorators = [
        { type: Component, args: [{
                    selector: '[nz-tabs-nav]',
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    template: "<div style=\"float:right;\" *ngIf=\"nzTabBarExtraContent\" class=\"ant-tabs-extra-content\">\n  <ng-template [ngTemplateOutlet]=\"nzTabBarExtraContent\"></ng-template>\n</div>\n<div class=\"ant-tabs-nav-container\"\n  [class.ant-tabs-nav-container-scrolling]=\"showPaginationControls\"\n  #navContainerElement>\n  <span class=\"ant-tabs-tab-prev\"\n    (click)=\"scrollHeader('before')\"\n    [class.ant-tabs-tab-btn-disabled]=\"disableScrollBefore\"\n    [class.ant-tabs-tab-arrow-show]=\"showPaginationControls\">\n    <span class=\"ant-tabs-tab-prev-icon\">\n      <i nz-icon [type]=\"nzPositionMode === 'horizontal' ? 'left' : 'up'\" class=\"ant-tabs-tab-prev-icon-target\"></i>\n    </span>\n  </span>\n  <span class=\"ant-tabs-tab-next\"\n    (click)=\"scrollHeader('after')\"\n    [class.ant-tabs-tab-btn-disabled]=\"disableScrollAfter\"\n    [class.ant-tabs-tab-arrow-show]=\"showPaginationControls\">\n    <span class=\"ant-tabs-tab-next-icon\">\n      <i nz-icon [type]=\"nzPositionMode === 'horizontal' ? 'right' : 'down'\" class=\"ant-tabs-tab-next-icon-target\"></i>\n    </span>\n  </span>\n  <div class=\"ant-tabs-nav-wrap\">\n    <div class=\"ant-tabs-nav-scroll\" #scrollListElement>\n      <div class=\"ant-tabs-nav\"\n        [class.ant-tabs-nav-animated]=\"nzAnimated\"\n        #navListElement\n        (cdkObserveContent)=\"onContentChanges()\">\n        <div>\n          <ng-content></ng-content>\n        </div>\n        <div nz-tabs-ink-bar [hidden]=\"nzHideBar\" [nzAnimated]=\"nzAnimated\" [nzPositionMode]=\"nzPositionMode\" style=\"display: block;\"></div>\n      </div>\n    </div>\n  </div>\n</div>"
                }] }
    ];
    /** @nocollapse */
    NzTabsNavComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: NgZone },
        { type: Renderer2 },
        { type: ChangeDetectorRef },
        { type: Directionality, decorators: [{ type: Optional }] }
    ]; };
    NzTabsNavComponent.propDecorators = {
        listOfNzTabLabelDirective: [{ type: ContentChildren, args: [NzTabLabelDirective,] }],
        nzTabsInkBarDirective: [{ type: ViewChild, args: [NzTabsInkBarDirective,] }],
        navContainerElement: [{ type: ViewChild, args: ['navContainerElement',] }],
        navListElement: [{ type: ViewChild, args: ['navListElement',] }],
        scrollListElement: [{ type: ViewChild, args: ['scrollListElement',] }],
        nzOnNextClick: [{ type: Output }],
        nzOnPrevClick: [{ type: Output }],
        nzTabBarExtraContent: [{ type: Input }],
        nzAnimated: [{ type: Input }],
        nzHideBar: [{ type: Input }],
        nzShowPagination: [{ type: Input }],
        nzType: [{ type: Input }],
        nzPositionMode: [{ type: Input }],
        selectedIndex: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTabsNavComponent.prototype, "nzAnimated", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTabsNavComponent.prototype, "nzHideBar", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzTabsNavComponent.prototype, "nzShowPagination", void 0);
    return NzTabsNavComponent;
}());
export { NzTabsNavComponent };
if (false) {
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype._tabPositionMode;
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype._scrollDistance;
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype._selectedIndex;
    /**
     * Cached text content of the header.
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype.currentTextContent;
    /** @type {?} */
    NzTabsNavComponent.prototype.showPaginationControls;
    /** @type {?} */
    NzTabsNavComponent.prototype.disableScrollAfter;
    /** @type {?} */
    NzTabsNavComponent.prototype.disableScrollBefore;
    /** @type {?} */
    NzTabsNavComponent.prototype.selectedIndexChanged;
    /** @type {?} */
    NzTabsNavComponent.prototype.realignInkBar;
    /** @type {?} */
    NzTabsNavComponent.prototype.tabLabelCount;
    /** @type {?} */
    NzTabsNavComponent.prototype.scrollDistanceChanged;
    /** @type {?} */
    NzTabsNavComponent.prototype.listOfNzTabLabelDirective;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzTabsInkBarDirective;
    /** @type {?} */
    NzTabsNavComponent.prototype.navContainerElement;
    /** @type {?} */
    NzTabsNavComponent.prototype.navListElement;
    /** @type {?} */
    NzTabsNavComponent.prototype.scrollListElement;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzOnNextClick;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzOnPrevClick;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzTabBarExtraContent;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzAnimated;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzHideBar;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzShowPagination;
    /** @type {?} */
    NzTabsNavComponent.prototype.nzType;
    /** @type {?} */
    NzTabsNavComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzTabsNavComponent.prototype.dir;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdGFicy1uYXYuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRhYnMvbnotdGFicy1uYXYuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7OztBQUNBLE9BQU8sRUFBYSxjQUFjLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUM5RCxPQUFPLEVBR0wsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsZUFBZSxFQUNmLFVBQVUsRUFDVixZQUFZLEVBQ1osS0FBSyxFQUNMLE1BQU0sRUFDTixRQUFRLEVBQ1IsTUFBTSxFQUNOLFNBQVMsRUFDVCxTQUFTLEVBQ1QsV0FBVyxFQUNYLFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFDdkIsT0FBTyxFQUFFLFNBQVMsRUFBRSxLQUFLLEVBQUUsRUFBRSxJQUFJLFlBQVksRUFBZ0IsTUFBTSxNQUFNLENBQUM7QUFDMUUsT0FBTyxFQUFFLFNBQVMsRUFBRSxTQUFTLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUV0RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFFcEQsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sMEJBQTBCLENBQUM7QUFDL0QsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sNkJBQTZCLENBQUM7O0lBRzlELHNCQUFzQixHQUFHLEVBQUU7QUFHakM7SUEwREUsNEJBQ1MsVUFBc0IsRUFDckIsTUFBYyxFQUNkLFFBQW1CLEVBQ25CLEdBQXNCLEVBQ1YsR0FBbUI7UUFKaEMsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUNyQixXQUFNLEdBQU4sTUFBTSxDQUFRO1FBQ2QsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQUNuQixRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQUNWLFFBQUcsR0FBSCxHQUFHLENBQWdCO1FBdkRqQyxxQkFBZ0IsR0FBc0IsWUFBWSxDQUFDO1FB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QUFLO21DQUNMLEtBQUs7eUJBQ0wsS0FBSztpQ0FFTCxLQUFLO2dDQWVMLEtBQUs7O0lBcEJtQjtRQUFmLFlBQVksRUFBRTs7MERBQW1CO0lBQ2xCO1FBQWYsWUFBWSxFQUFFOzt5REFBbUI7SUFDbEI7UUFBZixZQUFZLEVBQUU7O2dFQUF5QjtJQWdQbkQseUJBQUM7Q0FBQSxBQTlRRCxJQThRQztTQXZRWSxrQkFBa0I7Ozs7OztJQUM3Qiw4Q0FBMkQ7Ozs7O0lBQzNELDZDQUE0Qjs7Ozs7SUFDNUIsNENBQTJCOzs7Ozs7SUFFM0IsZ0RBQW1DOztJQUNuQyxvREFBK0I7O0lBQy9CLGdEQUEwQjs7SUFDMUIsaURBQTJCOztJQUMzQixrREFBNkI7O0lBQzdCLDJDQUEwQzs7SUFDMUMsMkNBQXNCOztJQUN0QixtREFBK0I7O0lBQy9CLHVEQUFnRzs7SUFDaEcsbURBQStFOztJQUMvRSxpREFBa0U7O0lBQ2xFLDRDQUF3RDs7SUFDeEQsK0NBQThEOztJQUM5RCwyQ0FBNEQ7O0lBQzVELDJDQUE0RDs7SUFDNUQsa0RBQWlEOztJQUNqRCx3Q0FBMkM7O0lBQzNDLHVDQUEyQzs7SUFDM0MsOENBQWlEOztJQUNqRCxvQ0FBeUI7O0lBNEJ2Qix3Q0FBNkI7Ozs7O0lBQzdCLG9DQUFzQjs7Ozs7SUFDdEIsc0NBQTJCOzs7OztJQUMzQixpQ0FBOEI7Ozs7O0lBQzlCLGlDQUF1QyIsInNvdXJjZXNDb250ZW50IjpbIi8qKiBjb2RlIGZyb20gaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvbWF0ZXJpYWwyICovXG5pbXBvcnQgeyBEaXJlY3Rpb24sIERpcmVjdGlvbmFsaXR5IH0gZnJvbSAnQGFuZ3VsYXIvY2RrL2JpZGknO1xuaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50Q2hlY2tlZCxcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZHJlbixcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBJbnB1dCxcbiAgTmdab25lLFxuICBPcHRpb25hbCxcbiAgT3V0cHV0LFxuICBRdWVyeUxpc3QsXG4gIFJlbmRlcmVyMixcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBmcm9tRXZlbnQsIG1lcmdlLCBvZiBhcyBvYnNlcnZhYmxlT2YsIFN1YnNjcmlwdGlvbiB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgYXVkaXRUaW1lLCBzdGFydFdpdGggfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcblxuaW1wb3J0IHsgTnpUYWJMYWJlbERpcmVjdGl2ZSB9IGZyb20gJy4vbnotdGFiLWxhYmVsLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOelRhYnNJbmtCYXJEaXJlY3RpdmUgfSBmcm9tICcuL256LXRhYnMtaW5rLWJhci5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpUYWJQb3NpdGlvbk1vZGUgfSBmcm9tICcuL256LXRhYnNldC5jb21wb25lbnQnO1xuXG5jb25zdCBFWEFHR0VSQVRFRF9PVkVSU0NST0xMID0gNjQ7XG5leHBvcnQgdHlwZSBTY3JvbGxEaXJlY3Rpb24gPSAnYWZ0ZXInIHwgJ2JlZm9yZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ1tuei10YWJzLW5hdl0nLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei10YWJzLW5hdi5jb21wb25lbnQuaHRtbCdcbn0pXG5leHBvcnQgY2xhc3MgTnpUYWJzTmF2Q29tcG9uZW50IGltcGxlbWVudHMgQWZ0ZXJDb250ZW50Q2hlY2tlZCwgQWZ0ZXJDb250ZW50SW5pdCB7XG4gIHByaXZhdGUgX3RhYlBvc2l0aW9uTW9kZTogTnpUYWJQb3NpdGlvbk1vZGUgPSAnaG9yaXpvbnRhbCc7XG4gIHByaXZhdGUgX3Njcm9sbERpc3RhbmNlID0gMDtcbiAgcHJpdmF0ZSBfc2VsZWN0ZWRJbmRleCA9IDA7XG4gIC8qKiBDYWNoZWQgdGV4dCBjb250ZW50IG9mIHRoZSBoZWFkZXIuICovXG4gIHByaXZhdGUgY3VycmVudFRleHRDb250ZW50OiBzdHJpbmc7XG4gIHNob3dQYWdpbmF0aW9uQ29udHJvbHMgPSBmYWxzZTtcbiAgZGlzYWJsZVNjcm9sbEFmdGVyID0gdHJ1ZTtcbiAgZGlzYWJsZVNjcm9sbEJlZm9yZSA9IHRydWU7XG4gIHNlbGVjdGVkSW5kZXhDaGFuZ2VkID0gZmFsc2U7XG4gIHJlYWxpZ25JbmtCYXI6IFN1YnNjcmlwdGlvbiB8IG51bGwgPSBudWxsO1xuICB0YWJMYWJlbENvdW50OiBudW1iZXI7XG4gIHNjcm9sbERpc3RhbmNlQ2hhbmdlZDogYm9vbGVhbjtcbiAgQENvbnRlbnRDaGlsZHJlbihOelRhYkxhYmVsRGlyZWN0aXZlKSBsaXN0T2ZOelRhYkxhYmVsRGlyZWN0aXZlOiBRdWVyeUxpc3Q8TnpUYWJMYWJlbERpcmVjdGl2ZT47XG4gIEBWaWV3Q2hpbGQoTnpUYWJzSW5rQmFyRGlyZWN0aXZlKSBuelRhYnNJbmtCYXJEaXJlY3RpdmU6IE56VGFic0lua0JhckRpcmVjdGl2ZTtcbiAgQFZpZXdDaGlsZCgnbmF2Q29udGFpbmVyRWxlbWVudCcpIG5hdkNvbnRhaW5lckVsZW1lbnQ6IEVsZW1lbnRSZWY7XG4gIEBWaWV3Q2hpbGQoJ25hdkxpc3RFbGVtZW50JykgbmF2TGlzdEVsZW1lbnQ6IEVsZW1lbnRSZWY7XG4gIEBWaWV3Q2hpbGQoJ3Njcm9sbExpc3RFbGVtZW50Jykgc2Nyb2xsTGlzdEVsZW1lbnQ6IEVsZW1lbnRSZWY7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9uTmV4dENsaWNrID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpPblByZXZDbGljayA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcbiAgQElucHV0KCkgbnpUYWJCYXJFeHRyYUNvbnRlbnQ6IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpBbmltYXRlZCA9IHRydWU7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekhpZGVCYXIgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1BhZ2luYXRpb24gPSB0cnVlO1xuICBASW5wdXQoKSBuelR5cGUgPSAnbGluZSc7XG5cbiAgQElucHV0KClcbiAgc2V0IG56UG9zaXRpb25Nb2RlKHZhbHVlOiBOelRhYlBvc2l0aW9uTW9kZSkge1xuICAgIHRoaXMuX3RhYlBvc2l0aW9uTW9kZSA9IHZhbHVlO1xuICAgIHRoaXMuYWxpZ25JbmtCYXJUb1NlbGVjdGVkVGFiKCk7XG4gICAgaWYgKHRoaXMubnpTaG93UGFnaW5hdGlvbikge1xuICAgICAgUHJvbWlzZS5yZXNvbHZlKCkudGhlbigoKSA9PiB7XG4gICAgICAgIHRoaXMudXBkYXRlUGFnaW5hdGlvbigpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgZ2V0IG56UG9zaXRpb25Nb2RlKCk6IE56VGFiUG9zaXRpb25Nb2RlIHtcbiAgICByZXR1cm4gdGhpcy5fdGFiUG9zaXRpb25Nb2RlO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IHNlbGVjdGVkSW5kZXgodmFsdWU6IG51bWJlcikge1xuICAgIHRoaXMuc2VsZWN0ZWRJbmRleENoYW5nZWQgPSB0aGlzLl9zZWxlY3RlZEluZGV4ICE9PSB2YWx1ZTtcbiAgICB0aGlzLl9zZWxlY3RlZEluZGV4ID0gdmFsdWU7XG4gIH1cblxuICBnZXQgc2VsZWN0ZWRJbmRleCgpOiBudW1iZXIge1xuICAgIHJldHVybiB0aGlzLl9zZWxlY3RlZEluZGV4O1xuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHVibGljIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgcHJpdmF0ZSBuZ1pvbmU6IE5nWm9uZSxcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIEBPcHRpb25hbCgpIHByaXZhdGUgZGlyOiBEaXJlY3Rpb25hbGl0eVxuICApIHt9XG5cbiAgb25Db250ZW50Q2hhbmdlcygpOiB2b2lkIHtcbiAgICBjb25zdCB0ZXh0Q29udGVudCA9IHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LnRleHRDb250ZW50O1xuICAgIC8vIFdlIG5lZWQgdG8gZGlmZiB0aGUgdGV4dCBjb250ZW50IG9mIHRoZSBoZWFkZXIsIGJlY2F1c2UgdGhlIE11dGF0aW9uT2JzZXJ2ZXIgY2FsbGJhY2tcbiAgICAvLyB3aWxsIGZpcmUgZXZlbiBpZiB0aGUgdGV4dCBjb250ZW50IGRpZG4ndCBjaGFuZ2Ugd2hpY2ggaXMgaW5lZmZpY2llbnQgYW5kIGlzIHByb25lXG4gICAgLy8gdG8gaW5maW5pdGUgbG9vcHMgaWYgYSBwb29ybHkgY29uc3RydWN0ZWQgZXhwcmVzc2lvbiBpcyBwYXNzZWQgaW4gKHNlZSAjMTQyNDkpLlxuICAgIGlmICh0ZXh0Q29udGVudCAhPT0gdGhpcy5jdXJyZW50VGV4dENvbnRlbnQpIHtcbiAgICAgIHRoaXMubmdab25lLnJ1bigoKSA9PiB7XG4gICAgICAgIGlmICh0aGlzLm56U2hvd1BhZ2luYXRpb24pIHtcbiAgICAgICAgICB0aGlzLnVwZGF0ZVBhZ2luYXRpb24oKTtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLmFsaWduSW5rQmFyVG9TZWxlY3RlZFRhYigpO1xuICAgICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICAgIH0pO1xuICAgIH1cbiAgfVxuXG4gIHNjcm9sbEhlYWRlcihzY3JvbGxEaXI6IFNjcm9sbERpcmVjdGlvbik6IHZvaWQge1xuICAgIGlmIChzY3JvbGxEaXIgPT09ICdiZWZvcmUnICYmICF0aGlzLmRpc2FibGVTY3JvbGxCZWZvcmUpIHtcbiAgICAgIHRoaXMubnpPblByZXZDbGljay5lbWl0KCk7XG4gICAgfSBlbHNlIGlmIChzY3JvbGxEaXIgPT09ICdhZnRlcicgJiYgIXRoaXMuZGlzYWJsZVNjcm9sbEFmdGVyKSB7XG4gICAgICB0aGlzLm56T25OZXh0Q2xpY2suZW1pdCgpO1xuICAgIH1cbiAgICAvLyBNb3ZlIHRoZSBzY3JvbGwgZGlzdGFuY2Ugb25lLXRoaXJkIHRoZSBsZW5ndGggb2YgdGhlIHRhYiBsaXN0J3Mgdmlld3BvcnQuXG4gICAgdGhpcy5zY3JvbGxEaXN0YW5jZSArPSAoKHNjcm9sbERpciA9PT0gJ2JlZm9yZScgPyAtMSA6IDEpICogdGhpcy52aWV3V2lkdGhIZWlnaHRQaXgpIC8gMztcbiAgfVxuXG4gIG5nQWZ0ZXJDb250ZW50Q2hlY2tlZCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy50YWJMYWJlbENvdW50ICE9PSB0aGlzLmxpc3RPZk56VGFiTGFiZWxEaXJlY3RpdmUubGVuZ3RoKSB7XG4gICAgICBpZiAodGhpcy5uelNob3dQYWdpbmF0aW9uKSB7XG4gICAgICAgIHRoaXMudXBkYXRlUGFnaW5hdGlvbigpO1xuICAgICAgfVxuICAgICAgdGhpcy50YWJMYWJlbENvdW50ID0gdGhpcy5saXN0T2ZOelRhYkxhYmVsRGlyZWN0aXZlLmxlbmd0aDtcbiAgICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICAgIH1cbiAgICBpZiAodGhpcy5zZWxlY3RlZEluZGV4Q2hhbmdlZCkge1xuICAgICAgdGhpcy5zY3JvbGxUb0xhYmVsKHRoaXMuX3NlbGVjdGVkSW5kZXgpO1xuICAgICAgaWYgKHRoaXMubnpTaG93UGFnaW5hdGlvbikge1xuICAgICAgICB0aGlzLmNoZWNrU2Nyb2xsaW5nQ29udHJvbHMoKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuYWxpZ25JbmtCYXJUb1NlbGVjdGVkVGFiKCk7XG4gICAgICB0aGlzLnNlbGVjdGVkSW5kZXhDaGFuZ2VkID0gZmFsc2U7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gICAgaWYgKHRoaXMuc2Nyb2xsRGlzdGFuY2VDaGFuZ2VkKSB7XG4gICAgICBpZiAodGhpcy5uelNob3dQYWdpbmF0aW9uKSB7XG4gICAgICAgIHRoaXMudXBkYXRlVGFiU2Nyb2xsUG9zaXRpb24oKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuc2Nyb2xsRGlzdGFuY2VDaGFuZ2VkID0gZmFsc2U7XG4gICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gIH1cblxuICBuZ0FmdGVyQ29udGVudEluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5yZWFsaWduSW5rQmFyID0gdGhpcy5uZ1pvbmUucnVuT3V0c2lkZUFuZ3VsYXIoKCkgPT4ge1xuICAgICAgY29uc3QgZGlyQ2hhbmdlID0gdGhpcy5kaXIgPyB0aGlzLmRpci5jaGFuZ2UgOiBvYnNlcnZhYmxlT2YobnVsbCk7XG4gICAgICBjb25zdCByZXNpemUgPVxuICAgICAgICB0eXBlb2Ygd2luZG93ICE9PSAndW5kZWZpbmVkJyA/IGZyb21FdmVudCh3aW5kb3csICdyZXNpemUnKS5waXBlKGF1ZGl0VGltZSgxMCkpIDogb2JzZXJ2YWJsZU9mKG51bGwpO1xuICAgICAgcmV0dXJuIG1lcmdlKGRpckNoYW5nZSwgcmVzaXplKVxuICAgICAgICAucGlwZShzdGFydFdpdGgobnVsbCkpXG4gICAgICAgIC5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgICAgIGlmICh0aGlzLm56U2hvd1BhZ2luYXRpb24pIHtcbiAgICAgICAgICAgIHRoaXMudXBkYXRlUGFnaW5hdGlvbigpO1xuICAgICAgICAgIH1cbiAgICAgICAgICB0aGlzLmFsaWduSW5rQmFyVG9TZWxlY3RlZFRhYigpO1xuICAgICAgICB9KTtcbiAgICB9KTtcbiAgfVxuXG4gIHVwZGF0ZVRhYlNjcm9sbFBvc2l0aW9uKCk6IHZvaWQge1xuICAgIGNvbnN0IHNjcm9sbERpc3RhbmNlID0gdGhpcy5zY3JvbGxEaXN0YW5jZTtcbiAgICBpZiAodGhpcy5uelBvc2l0aW9uTW9kZSA9PT0gJ2hvcml6b250YWwnKSB7XG4gICAgICBjb25zdCB0cmFuc2xhdGVYID0gdGhpcy5nZXRMYXlvdXREaXJlY3Rpb24oKSA9PT0gJ2x0cicgPyAtc2Nyb2xsRGlzdGFuY2UgOiBzY3JvbGxEaXN0YW5jZTtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5uYXZMaXN0RWxlbWVudC5uYXRpdmVFbGVtZW50LCAndHJhbnNmb3JtJywgYHRyYW5zbGF0ZTNkKCR7dHJhbnNsYXRlWH1weCwgMCwgMClgKTtcbiAgICB9IGVsc2Uge1xuICAgICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLm5hdkxpc3RFbGVtZW50Lm5hdGl2ZUVsZW1lbnQsICd0cmFuc2Zvcm0nLCBgdHJhbnNsYXRlM2QoMCwkey1zY3JvbGxEaXN0YW5jZX1weCwgMClgKTtcbiAgICB9XG4gIH1cblxuICB1cGRhdGVQYWdpbmF0aW9uKCk6IHZvaWQge1xuICAgIHRoaXMuY2hlY2tQYWdpbmF0aW9uRW5hYmxlZCgpO1xuICAgIHRoaXMuY2hlY2tTY3JvbGxpbmdDb250cm9scygpO1xuICAgIHRoaXMudXBkYXRlVGFiU2Nyb2xsUG9zaXRpb24oKTtcbiAgfVxuXG4gIGNoZWNrUGFnaW5hdGlvbkVuYWJsZWQoKTogdm9pZCB7XG4gICAgY29uc3QgaXNFbmFibGVkID0gdGhpcy50YWJMaXN0U2Nyb2xsV2lkdGhIZWlnaHRQaXggPiB0aGlzLnRhYkxpc3RTY3JvbGxPZmZTZXRXaWR0aEhlaWdodDtcbiAgICBpZiAoIWlzRW5hYmxlZCkge1xuICAgICAgdGhpcy5zY3JvbGxEaXN0YW5jZSA9IDA7XG4gICAgfVxuICAgIGlmIChpc0VuYWJsZWQgIT09IHRoaXMuc2hvd1BhZ2luYXRpb25Db250cm9scykge1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfVxuICAgIHRoaXMuc2hvd1BhZ2luYXRpb25Db250cm9scyA9IGlzRW5hYmxlZDtcbiAgfVxuXG4gIHNjcm9sbFRvTGFiZWwobGFiZWxJbmRleDogbnVtYmVyKTogdm9pZCB7XG4gICAgY29uc3Qgc2VsZWN0ZWRMYWJlbCA9IHRoaXMubGlzdE9mTnpUYWJMYWJlbERpcmVjdGl2ZSA/IHRoaXMubGlzdE9mTnpUYWJMYWJlbERpcmVjdGl2ZS50b0FycmF5KClbbGFiZWxJbmRleF0gOiBudWxsO1xuXG4gICAgaWYgKHNlbGVjdGVkTGFiZWwpIHtcbiAgICAgIC8vIFRoZSB2aWV3IGxlbmd0aCBpcyB0aGUgdmlzaWJsZSB3aWR0aCBvZiB0aGUgdGFiIGxhYmVscy5cblxuICAgICAgbGV0IGxhYmVsQmVmb3JlUG9zOiBudW1iZXI7XG4gICAgICBsZXQgbGFiZWxBZnRlclBvczogbnVtYmVyO1xuICAgICAgaWYgKHRoaXMubnpQb3NpdGlvbk1vZGUgPT09ICdob3Jpem9udGFsJykge1xuICAgICAgICBpZiAodGhpcy5nZXRMYXlvdXREaXJlY3Rpb24oKSA9PT0gJ2x0cicpIHtcbiAgICAgICAgICBsYWJlbEJlZm9yZVBvcyA9IHNlbGVjdGVkTGFiZWwuZ2V0T2Zmc2V0TGVmdCgpO1xuICAgICAgICAgIGxhYmVsQWZ0ZXJQb3MgPSBsYWJlbEJlZm9yZVBvcyArIHNlbGVjdGVkTGFiZWwuZ2V0T2Zmc2V0V2lkdGgoKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBsYWJlbEFmdGVyUG9zID0gdGhpcy5uYXZMaXN0RWxlbWVudC5uYXRpdmVFbGVtZW50Lm9mZnNldFdpZHRoIC0gc2VsZWN0ZWRMYWJlbC5nZXRPZmZzZXRMZWZ0KCk7XG4gICAgICAgICAgbGFiZWxCZWZvcmVQb3MgPSBsYWJlbEFmdGVyUG9zIC0gc2VsZWN0ZWRMYWJlbC5nZXRPZmZzZXRXaWR0aCgpO1xuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICBsYWJlbEJlZm9yZVBvcyA9IHNlbGVjdGVkTGFiZWwuZ2V0T2Zmc2V0VG9wKCk7XG4gICAgICAgIGxhYmVsQWZ0ZXJQb3MgPSBsYWJlbEJlZm9yZVBvcyArIHNlbGVjdGVkTGFiZWwuZ2V0T2Zmc2V0SGVpZ2h0KCk7XG4gICAgICB9XG4gICAgICBjb25zdCBiZWZvcmVWaXNpYmxlUG9zID0gdGhpcy5zY3JvbGxEaXN0YW5jZTtcbiAgICAgIGNvbnN0IGFmdGVyVmlzaWJsZVBvcyA9IHRoaXMuc2Nyb2xsRGlzdGFuY2UgKyB0aGlzLnZpZXdXaWR0aEhlaWdodFBpeDtcblxuICAgICAgaWYgKGxhYmVsQmVmb3JlUG9zIDwgYmVmb3JlVmlzaWJsZVBvcykge1xuICAgICAgICAvLyBTY3JvbGwgaGVhZGVyIHRvIG1vdmUgbGFiZWwgdG8gdGhlIGJlZm9yZSBkaXJlY3Rpb25cbiAgICAgICAgdGhpcy5zY3JvbGxEaXN0YW5jZSAtPSBiZWZvcmVWaXNpYmxlUG9zIC0gbGFiZWxCZWZvcmVQb3MgKyBFWEFHR0VSQVRFRF9PVkVSU0NST0xMO1xuICAgICAgfSBlbHNlIGlmIChsYWJlbEFmdGVyUG9zID4gYWZ0ZXJWaXNpYmxlUG9zKSB7XG4gICAgICAgIC8vIFNjcm9sbCBoZWFkZXIgdG8gbW92ZSBsYWJlbCB0byB0aGUgYWZ0ZXIgZGlyZWN0aW9uXG4gICAgICAgIHRoaXMuc2Nyb2xsRGlzdGFuY2UgKz0gbGFiZWxBZnRlclBvcyAtIGFmdGVyVmlzaWJsZVBvcyArIEVYQUdHRVJBVEVEX09WRVJTQ1JPTEw7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgY2hlY2tTY3JvbGxpbmdDb250cm9scygpOiB2b2lkIHtcbiAgICAvLyBDaGVjayBpZiB0aGUgcGFnaW5hdGlvbiBhcnJvd3Mgc2hvdWxkIGJlIGFjdGl2YXRlZC5cbiAgICB0aGlzLmRpc2FibGVTY3JvbGxCZWZvcmUgPSB0aGlzLnNjcm9sbERpc3RhbmNlID09PSAwO1xuICAgIHRoaXMuZGlzYWJsZVNjcm9sbEFmdGVyID0gdGhpcy5zY3JvbGxEaXN0YW5jZSA9PT0gdGhpcy5nZXRNYXhTY3JvbGxEaXN0YW5jZSgpO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgLyoqXG4gICAqIERldGVybWluZXMgd2hhdCBpcyB0aGUgbWF4aW11bSBsZW5ndGggaW4gcGl4ZWxzIHRoYXQgY2FuIGJlIHNldCBmb3IgdGhlIHNjcm9sbCBkaXN0YW5jZS4gVGhpc1xuICAgKiBpcyBlcXVhbCB0byB0aGUgZGlmZmVyZW5jZSBpbiB3aWR0aCBiZXR3ZWVuIHRoZSB0YWIgbGlzdCBjb250YWluZXIgYW5kIHRhYiBoZWFkZXIgY29udGFpbmVyLlxuICAgKlxuICAgKiBUaGlzIGlzIGFuIGV4cGVuc2l2ZSBjYWxsIHRoYXQgZm9yY2VzIGEgbGF5b3V0IHJlZmxvdyB0byBjb21wdXRlIGJveCBhbmQgc2Nyb2xsIG1ldHJpY3MgYW5kXG4gICAqIHNob3VsZCBiZSBjYWxsZWQgc3BhcmluZ2x5LlxuICAgKi9cbiAgZ2V0TWF4U2Nyb2xsRGlzdGFuY2UoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy50YWJMaXN0U2Nyb2xsV2lkdGhIZWlnaHRQaXggLSB0aGlzLnZpZXdXaWR0aEhlaWdodFBpeCB8fCAwO1xuICB9XG5cbiAgLyoqIFNldHMgdGhlIGRpc3RhbmNlIGluIHBpeGVscyB0aGF0IHRoZSB0YWIgaGVhZGVyIHNob3VsZCBiZSB0cmFuc2Zvcm1lZCBpbiB0aGUgWC1heGlzLiAqL1xuICBzZXQgc2Nyb2xsRGlzdGFuY2UodjogbnVtYmVyKSB7XG4gICAgdGhpcy5fc2Nyb2xsRGlzdGFuY2UgPSBNYXRoLm1heCgwLCBNYXRoLm1pbih0aGlzLmdldE1heFNjcm9sbERpc3RhbmNlKCksIHYpKTtcblxuICAgIC8vIE1hcmsgdGhhdCB0aGUgc2Nyb2xsIGRpc3RhbmNlIGhhcyBjaGFuZ2VkIHNvIHRoYXQgYWZ0ZXIgdGhlIHZpZXcgaXMgY2hlY2tlZCwgdGhlIENTU1xuICAgIC8vIHRyYW5zZm9ybWF0aW9uIGNhbiBtb3ZlIHRoZSBoZWFkZXIuXG4gICAgdGhpcy5zY3JvbGxEaXN0YW5jZUNoYW5nZWQgPSB0cnVlO1xuXG4gICAgdGhpcy5jaGVja1Njcm9sbGluZ0NvbnRyb2xzKCk7XG4gIH1cblxuICBnZXQgc2Nyb2xsRGlzdGFuY2UoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5fc2Nyb2xsRGlzdGFuY2U7XG4gIH1cblxuICBnZXQgdmlld1dpZHRoSGVpZ2h0UGl4KCk6IG51bWJlciB7XG4gICAgbGV0IFBBR0lOQVRJT05fUElYID0gMDtcbiAgICBpZiAodGhpcy5zaG93UGFnaW5hdGlvbkNvbnRyb2xzKSB7XG4gICAgICBQQUdJTkFUSU9OX1BJWCA9IDY0O1xuICAgIH1cbiAgICBpZiAodGhpcy5uelBvc2l0aW9uTW9kZSA9PT0gJ2hvcml6b250YWwnKSB7XG4gICAgICByZXR1cm4gdGhpcy5uYXZDb250YWluZXJFbGVtZW50Lm5hdGl2ZUVsZW1lbnQub2Zmc2V0V2lkdGggLSBQQUdJTkFUSU9OX1BJWDtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMubmF2Q29udGFpbmVyRWxlbWVudC5uYXRpdmVFbGVtZW50Lm9mZnNldEhlaWdodCAtIFBBR0lOQVRJT05fUElYO1xuICAgIH1cbiAgfVxuXG4gIGdldCB0YWJMaXN0U2Nyb2xsV2lkdGhIZWlnaHRQaXgoKTogbnVtYmVyIHtcbiAgICBpZiAodGhpcy5uelBvc2l0aW9uTW9kZSA9PT0gJ2hvcml6b250YWwnKSB7XG4gICAgICByZXR1cm4gdGhpcy5uYXZMaXN0RWxlbWVudC5uYXRpdmVFbGVtZW50LnNjcm9sbFdpZHRoO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdGhpcy5uYXZMaXN0RWxlbWVudC5uYXRpdmVFbGVtZW50LnNjcm9sbEhlaWdodDtcbiAgICB9XG4gIH1cblxuICBnZXQgdGFiTGlzdFNjcm9sbE9mZlNldFdpZHRoSGVpZ2h0KCk6IG51bWJlciB7XG4gICAgaWYgKHRoaXMubnpQb3NpdGlvbk1vZGUgPT09ICdob3Jpem9udGFsJykge1xuICAgICAgcmV0dXJuIHRoaXMuc2Nyb2xsTGlzdEVsZW1lbnQubmF0aXZlRWxlbWVudC5vZmZzZXRXaWR0aDtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50Lm9mZnNldEhlaWdodDtcbiAgICB9XG4gIH1cblxuICBnZXRMYXlvdXREaXJlY3Rpb24oKTogRGlyZWN0aW9uIHtcbiAgICByZXR1cm4gdGhpcy5kaXIgJiYgdGhpcy5kaXIudmFsdWUgPT09ICdydGwnID8gJ3J0bCcgOiAnbHRyJztcbiAgfVxuXG4gIGFsaWduSW5rQmFyVG9TZWxlY3RlZFRhYigpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uelR5cGUgPT09ICdsaW5lJykge1xuICAgICAgY29uc3Qgc2VsZWN0ZWRMYWJlbFdyYXBwZXIgPVxuICAgICAgICB0aGlzLmxpc3RPZk56VGFiTGFiZWxEaXJlY3RpdmUgJiYgdGhpcy5saXN0T2ZOelRhYkxhYmVsRGlyZWN0aXZlLmxlbmd0aFxuICAgICAgICAgID8gdGhpcy5saXN0T2ZOelRhYkxhYmVsRGlyZWN0aXZlLnRvQXJyYXkoKVt0aGlzLnNlbGVjdGVkSW5kZXhdLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudFxuICAgICAgICAgIDogbnVsbDtcbiAgICAgIGlmICh0aGlzLm56VGFic0lua0JhckRpcmVjdGl2ZSkge1xuICAgICAgICB0aGlzLm56VGFic0lua0JhckRpcmVjdGl2ZS5hbGlnblRvRWxlbWVudChzZWxlY3RlZExhYmVsV3JhcHBlcik7XG4gICAgICB9XG4gICAgfVxuICB9XG59XG4iXX0=