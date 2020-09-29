/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChildren, ElementRef, HostBinding, Inject, Input, NgZone, Optional, QueryList, Renderer2, ViewChild, ViewEncapsulation } from '@angular/core';
import { ANIMATION_MODULE_TYPE } from '@angular/platform-browser/animations';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { isEmpty } from '../core/util/check';
import { InputBoolean } from '../core/util/convert';
import { findFirstNotEmptyNode, findLastNotEmptyNode } from '../core/util/dom';
import { NzWaveDirective, NZ_WAVE_GLOBAL_CONFIG } from '../core/wave/nz-wave.directive';
import { NzIconDirective } from '../icon/nz-icon.directive';
var NzButtonComponent = /** @class */ (function () {
    function NzButtonComponent(elementRef, cdr, renderer, nzUpdateHostClassService, ngZone, waveConfig, animationType) {
        this.elementRef = elementRef;
        this.cdr = cdr;
        this.renderer = renderer;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.ngZone = ngZone;
        this.waveConfig = waveConfig;
        this.animationType = animationType;
        this.el = this.elementRef.nativeElement;
        this.iconOnly = false;
        this.nzWave = new NzWaveDirective(this.ngZone, this.elementRef, this.waveConfig, this.animationType);
        this.nzBlock = false;
        this.nzGhost = false;
        this.nzSearch = false;
        this.nzLoading = false;
        this.nzType = 'default';
        this.nzShape = null;
        this.nzSize = 'default';
    }
    /** temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289 */
    /**
     * temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289
     * @return {?}
     */
    NzButtonComponent.prototype.setClassMap = /**
     * temp solution since no method add classMap to host https://github.com/angular/angular/issues/7289
     * @return {?}
     */
    function () {
        var _a;
        /** @type {?} */
        var prefixCls = 'ant-btn';
        /** @type {?} */
        var sizeMap = { large: 'lg', small: 'sm' };
        this.nzUpdateHostClassService.updateHostClass(this.el, (_a = {},
            _a["" + prefixCls] = true,
            _a[prefixCls + "-" + this.nzType] = this.nzType,
            _a[prefixCls + "-" + this.nzShape] = this.nzShape,
            _a[prefixCls + "-" + sizeMap[this.nzSize]] = sizeMap[this.nzSize],
            _a[prefixCls + "-loading"] = this.nzLoading,
            _a[prefixCls + "-icon-only"] = this.iconOnly,
            _a[prefixCls + "-background-ghost"] = this.nzGhost,
            _a[prefixCls + "-block"] = this.nzBlock,
            _a["ant-input-search-button"] = this.nzSearch,
            _a));
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzButtonComponent.prototype.updateIconDisplay = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (this.iconElement) {
            this.renderer.setStyle(this.iconElement, 'display', value ? 'none' : 'inline-block');
        }
    };
    /**
     * @return {?}
     */
    NzButtonComponent.prototype.checkContent = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var hasIcon = this.listOfIconElement && this.listOfIconElement.length;
        if (hasIcon) {
            this.moveIcon();
        }
        this.renderer.removeStyle(this.contentElement.nativeElement, 'display');
        /** https://github.com/angular/angular/issues/12530 **/
        if (isEmpty(this.contentElement.nativeElement)) {
            this.renderer.setStyle(this.contentElement.nativeElement, 'display', 'none');
            this.iconOnly = !!hasIcon;
        }
        else {
            this.renderer.removeStyle(this.contentElement.nativeElement, 'display');
            this.iconOnly = false;
        }
        this.setClassMap();
        this.updateIconDisplay(this.nzLoading);
        this.cdr.detectChanges();
    };
    /**
     * @return {?}
     */
    NzButtonComponent.prototype.moveIcon = /**
     * @return {?}
     */
    function () {
        if (this.listOfIconElement && this.listOfIconElement.length) {
            /** @type {?} */
            var firstChildElement = findFirstNotEmptyNode(this.contentElement.nativeElement);
            /** @type {?} */
            var lastChildElement = findLastNotEmptyNode(this.contentElement.nativeElement);
            if (firstChildElement && firstChildElement === this.listOfIconElement.first.nativeElement) {
                this.renderer.insertBefore(this.el, firstChildElement, this.contentElement.nativeElement);
                this.iconElement = (/** @type {?} */ (firstChildElement));
            }
            else if (lastChildElement && lastChildElement === this.listOfIconElement.last.nativeElement) {
                this.renderer.appendChild(this.el, lastChildElement);
            }
        }
    };
    /**
     * @return {?}
     */
    NzButtonComponent.prototype.ngAfterContentInit = /**
     * @return {?}
     */
    function () {
        this.checkContent();
    };
    /**
     * @return {?}
     */
    NzButtonComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.setClassMap();
        this.nzWave.ngOnInit();
    };
    /**
     * @return {?}
     */
    NzButtonComponent.prototype.ngOnDestroy = /**
     * @return {?}
     */
    function () {
        this.nzWave.ngOnDestroy();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzButtonComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.nzBlock ||
            changes.nzGhost ||
            changes.nzSearch ||
            changes.nzType ||
            changes.nzShape ||
            changes.nzSize ||
            changes.nzLoading) {
            this.setClassMap();
        }
        if (changes.nzLoading) {
            this.updateIconDisplay(this.nzLoading);
        }
    };
    NzButtonComponent.decorators = [
        { type: Component, args: [{
                    selector: '[nz-button]',
                    providers: [NzUpdateHostClassService],
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    template: "<i nz-icon type=\"loading\" *ngIf=\"nzLoading\"></i>\n<span (cdkObserveContent)=\"checkContent()\" #contentElement><ng-content></ng-content></span>"
                }] }
    ];
    /** @nocollapse */
    NzButtonComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: ChangeDetectorRef },
        { type: Renderer2 },
        { type: NzUpdateHostClassService },
        { type: NgZone },
        { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NZ_WAVE_GLOBAL_CONFIG,] }] },
        { type: String, decorators: [{ type: Optional }, { type: Inject, args: [ANIMATION_MODULE_TYPE,] }] }
    ]; };
    NzButtonComponent.propDecorators = {
        contentElement: [{ type: ViewChild, args: ['contentElement',] }],
        listOfIconElement: [{ type: ContentChildren, args: [NzIconDirective, { read: ElementRef },] }],
        nzWave: [{ type: HostBinding, args: ['attr.nz-wave',] }],
        nzBlock: [{ type: Input }],
        nzGhost: [{ type: Input }],
        nzSearch: [{ type: Input }],
        nzLoading: [{ type: Input }],
        nzType: [{ type: Input }],
        nzShape: [{ type: Input }],
        nzSize: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzButtonComponent.prototype, "nzBlock", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzButtonComponent.prototype, "nzGhost", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzButtonComponent.prototype, "nzSearch", void 0);
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzButtonComponent.prototype, "nzLoading", void 0);
    return NzButtonComponent;
}());
export { NzButtonComponent };
if (false) {
    /** @type {?} */
    NzButtonComponent.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.iconElement;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.iconOnly;
    /** @type {?} */
    NzButtonComponent.prototype.contentElement;
    /** @type {?} */
    NzButtonComponent.prototype.listOfIconElement;
    /** @type {?} */
    NzButtonComponent.prototype.nzWave;
    /** @type {?} */
    NzButtonComponent.prototype.nzBlock;
    /** @type {?} */
    NzButtonComponent.prototype.nzGhost;
    /** @type {?} */
    NzButtonComponent.prototype.nzSearch;
    /** @type {?} */
    NzButtonComponent.prototype.nzLoading;
    /** @type {?} */
    NzButtonComponent.prototype.nzType;
    /** @type {?} */
    NzButtonComponent.prototype.nzShape;
    /** @type {?} */
    NzButtonComponent.prototype.nzSize;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.renderer;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.nzUpdateHostClassService;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.waveConfig;
    /**
     * @type {?}
     * @private
     */
    NzButtonComponent.prototype.animationType;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYnV0dG9uLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJidXR0b24vbnotYnV0dG9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxlQUFlLEVBQ2YsVUFBVSxFQUNWLFdBQVcsRUFDWCxNQUFNLEVBQ04sS0FBSyxFQUNMLE1BQU0sRUFJTixRQUFRLEVBQ1IsU0FBUyxFQUNULFNBQVMsRUFFVCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLHNDQUFzQyxDQUFDO0FBRTdFLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBRXRGLE9BQU8sRUFBRSxPQUFPLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUM3QyxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sc0JBQXNCLENBQUM7QUFDcEQsT0FBTyxFQUFFLHFCQUFxQixFQUFFLG9CQUFvQixFQUFFLE1BQU0sa0JBQWtCLENBQUM7QUFDL0UsT0FBTyxFQUFnQixlQUFlLEVBQUUscUJBQXFCLEVBQUUsTUFBTSxnQ0FBZ0MsQ0FBQztBQUN0RyxPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFLNUQ7SUFtRkUsMkJBQ1UsVUFBc0IsRUFDdEIsR0FBc0IsRUFDdEIsUUFBbUIsRUFDbkIsd0JBQWtELEVBQ2xELE1BQWMsRUFDNkIsVUFBd0IsRUFDeEIsYUFBcUI7UUFOaEUsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0QixRQUFHLEdBQUgsR0FBRyxDQUFtQjtRQUN0QixhQUFRLEdBQVIsUUFBUSxDQUFXO1FBQ25CLDZCQUF3QixHQUF4Qix3QkFBd0IsQ0FBMEI7UUFDbEQsV0FBTSxHQUFOLE1BQU0sQ0FBUTtRQUM2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NEJBQ0wsS0FBSzt5QkFDTCxLQUFLOzBCQUNMLEtBQUs7eUJBQ0wsS0FBSzs7SUFObUI7UUFBZixZQUFZLEVBQUU7O3NEQUFpQjtJQUNoQjtRQUFmLFlBQVksRUFBRTs7c0RBQWlCO0lBQ2hCO1FBQWYsWUFBWSxFQUFFOzt1REFBa0I7SUFDakI7UUFBZixZQUFZLEVBQUU7O3dEQUFtQjtJQW1HN0Msd0JBQUM7Q0FBQSxBQTFIRCxJQTBIQztTQWxIWSxpQkFBaUI7OztJQUM1QiwrQkFBeUQ7Ozs7O0lBQ3pELHdDQUFpQzs7Ozs7SUFDakMscUNBQXlCOztJQUN6QiwyQ0FBd0Q7O0lBQ3hELDhDQUFpRzs7SUFDakcsbUNBS0U7O0lBQ0Ysb0NBQXlDOztJQUN6QyxvQ0FBeUM7O0lBQ3pDLHFDQUEwQzs7SUFDMUMsc0NBQTJDOztJQUMzQyxtQ0FBMEM7O0lBQzFDLG9DQUF1Qzs7SUFDdkMsbUNBQTJDOzs7OztJQTBEekMsdUNBQThCOzs7OztJQUM5QixnQ0FBOEI7Ozs7O0lBQzlCLHFDQUEyQjs7Ozs7SUFDM0IscURBQTBEOzs7OztJQUMxRCxtQ0FBc0I7Ozs7O0lBQ3RCLHVDQUEyRTs7Ozs7SUFDM0UsMENBQXdFIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJDb250ZW50SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIENvbnRlbnRDaGlsZHJlbixcbiAgRWxlbWVudFJlZixcbiAgSG9zdEJpbmRpbmcsXG4gIEluamVjdCxcbiAgSW5wdXQsXG4gIE5nWm9uZSxcbiAgT25DaGFuZ2VzLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3B0aW9uYWwsXG4gIFF1ZXJ5TGlzdCxcbiAgUmVuZGVyZXIyLFxuICBTaW1wbGVDaGFuZ2VzLFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgQU5JTUFUSU9OX01PRFVMRV9UWVBFIH0gZnJvbSAnQGFuZ3VsYXIvcGxhdGZvcm0tYnJvd3Nlci9hbmltYXRpb25zJztcblxuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IE56U2l6ZUxEU1R5cGUsIE56U2l6ZU1hcCB9IGZyb20gJy4uL2NvcmUvdHlwZXMvc2l6ZSc7XG5pbXBvcnQgeyBpc0VtcHR5IH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IGZpbmRGaXJzdE5vdEVtcHR5Tm9kZSwgZmluZExhc3ROb3RFbXB0eU5vZGUgfSBmcm9tICcuLi9jb3JlL3V0aWwvZG9tJztcbmltcG9ydCB7IE56V2F2ZUNvbmZpZywgTnpXYXZlRGlyZWN0aXZlLCBOWl9XQVZFX0dMT0JBTF9DT05GSUcgfSBmcm9tICcuLi9jb3JlL3dhdmUvbnotd2F2ZS5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpJY29uRGlyZWN0aXZlIH0gZnJvbSAnLi4vaWNvbi9uei1pY29uLmRpcmVjdGl2ZSc7XG5cbmV4cG9ydCB0eXBlIE56QnV0dG9uVHlwZSA9ICdwcmltYXJ5JyB8ICdkYXNoZWQnIHwgJ2RhbmdlcicgfCAnZGVmYXVsdCc7XG5leHBvcnQgdHlwZSBOekJ1dHRvblNoYXBlID0gJ2NpcmNsZScgfCAncm91bmQnIHwgbnVsbDtcblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnW256LWJ1dHRvbl0nLFxuICBwcm92aWRlcnM6IFtOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2VdLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1idXR0b24uY29tcG9uZW50Lmh0bWwnXG59KVxuZXhwb3J0IGNsYXNzIE56QnV0dG9uQ29tcG9uZW50IGltcGxlbWVudHMgQWZ0ZXJDb250ZW50SW5pdCwgT25Jbml0LCBPbkRlc3Ryb3ksIE9uQ2hhbmdlcyB7XG4gIHJlYWRvbmx5IGVsOiBIVE1MRWxlbWVudCA9IHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50O1xuICBwcml2YXRlIGljb25FbGVtZW50OiBIVE1MRWxlbWVudDtcbiAgcHJpdmF0ZSBpY29uT25seSA9IGZhbHNlO1xuICBAVmlld0NoaWxkKCdjb250ZW50RWxlbWVudCcpIGNvbnRlbnRFbGVtZW50OiBFbGVtZW50UmVmO1xuICBAQ29udGVudENoaWxkcmVuKE56SWNvbkRpcmVjdGl2ZSwgeyByZWFkOiBFbGVtZW50UmVmIH0pIGxpc3RPZkljb25FbGVtZW50OiBRdWVyeUxpc3Q8RWxlbWVudFJlZj47XG4gIEBIb3N0QmluZGluZygnYXR0ci5uei13YXZlJykgbnpXYXZlID0gbmV3IE56V2F2ZURpcmVjdGl2ZShcbiAgICB0aGlzLm5nWm9uZSxcbiAgICB0aGlzLmVsZW1lbnRSZWYsXG4gICAgdGhpcy53YXZlQ29uZmlnLFxuICAgIHRoaXMuYW5pbWF0aW9uVHlwZVxuICApO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpCbG9jayA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpHaG9zdCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpTZWFyY2ggPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56TG9hZGluZyA9IGZhbHNlO1xuICBASW5wdXQoKSBuelR5cGU6IE56QnV0dG9uVHlwZSA9ICdkZWZhdWx0JztcbiAgQElucHV0KCkgbnpTaGFwZTogTnpCdXR0b25TaGFwZSA9IG51bGw7XG4gIEBJbnB1dCgpIG56U2l6ZTogTnpTaXplTERTVHlwZSA9ICdkZWZhdWx0JztcblxuICAvKiogdGVtcCBzb2x1dGlvbiBzaW5jZSBubyBtZXRob2QgYWRkIGNsYXNzTWFwIHRvIGhvc3QgaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9pc3N1ZXMvNzI4OSAqL1xuICBzZXRDbGFzc01hcCgpOiB2b2lkIHtcbiAgICBjb25zdCBwcmVmaXhDbHMgPSAnYW50LWJ0bic7XG4gICAgY29uc3Qgc2l6ZU1hcDogTnpTaXplTWFwID0geyBsYXJnZTogJ2xnJywgc21hbGw6ICdzbScgfTtcbiAgICB0aGlzLm56VXBkYXRlSG9zdENsYXNzU2VydmljZS51cGRhdGVIb3N0Q2xhc3ModGhpcy5lbCwge1xuICAgICAgW2Ake3ByZWZpeENsc31gXTogdHJ1ZSxcbiAgICAgIFtgJHtwcmVmaXhDbHN9LSR7dGhpcy5uelR5cGV9YF06IHRoaXMubnpUeXBlLFxuICAgICAgW2Ake3ByZWZpeENsc30tJHt0aGlzLm56U2hhcGV9YF06IHRoaXMubnpTaGFwZSxcbiAgICAgIFtgJHtwcmVmaXhDbHN9LSR7c2l6ZU1hcFt0aGlzLm56U2l6ZV19YF06IHNpemVNYXBbdGhpcy5uelNpemVdLFxuICAgICAgW2Ake3ByZWZpeENsc30tbG9hZGluZ2BdOiB0aGlzLm56TG9hZGluZyxcbiAgICAgIFtgJHtwcmVmaXhDbHN9LWljb24tb25seWBdOiB0aGlzLmljb25Pbmx5LFxuICAgICAgW2Ake3ByZWZpeENsc30tYmFja2dyb3VuZC1naG9zdGBdOiB0aGlzLm56R2hvc3QsXG4gICAgICBbYCR7cHJlZml4Q2xzfS1ibG9ja2BdOiB0aGlzLm56QmxvY2ssXG4gICAgICBbYGFudC1pbnB1dC1zZWFyY2gtYnV0dG9uYF06IHRoaXMubnpTZWFyY2hcbiAgICB9KTtcbiAgfVxuXG4gIHVwZGF0ZUljb25EaXNwbGF5KHZhbHVlOiBib29sZWFuKTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaWNvbkVsZW1lbnQpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5pY29uRWxlbWVudCwgJ2Rpc3BsYXknLCB2YWx1ZSA/ICdub25lJyA6ICdpbmxpbmUtYmxvY2snKTtcbiAgICB9XG4gIH1cblxuICBjaGVja0NvbnRlbnQoKTogdm9pZCB7XG4gICAgY29uc3QgaGFzSWNvbiA9IHRoaXMubGlzdE9mSWNvbkVsZW1lbnQgJiYgdGhpcy5saXN0T2ZJY29uRWxlbWVudC5sZW5ndGg7XG4gICAgaWYgKGhhc0ljb24pIHtcbiAgICAgIHRoaXMubW92ZUljb24oKTtcbiAgICB9XG4gICAgdGhpcy5yZW5kZXJlci5yZW1vdmVTdHlsZSh0aGlzLmNvbnRlbnRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQsICdkaXNwbGF5Jyk7XG4gICAgLyoqIGh0dHBzOi8vZ2l0aHViLmNvbS9hbmd1bGFyL2FuZ3VsYXIvaXNzdWVzLzEyNTMwICoqL1xuICAgIGlmIChpc0VtcHR5KHRoaXMuY29udGVudEVsZW1lbnQubmF0aXZlRWxlbWVudCkpIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5jb250ZW50RWxlbWVudC5uYXRpdmVFbGVtZW50LCAnZGlzcGxheScsICdub25lJyk7XG4gICAgICB0aGlzLmljb25Pbmx5ID0gISFoYXNJY29uO1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnJlbW92ZVN0eWxlKHRoaXMuY29udGVudEVsZW1lbnQubmF0aXZlRWxlbWVudCwgJ2Rpc3BsYXknKTtcbiAgICAgIHRoaXMuaWNvbk9ubHkgPSBmYWxzZTtcbiAgICB9XG4gICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIHRoaXMudXBkYXRlSWNvbkRpc3BsYXkodGhpcy5uekxvYWRpbmcpO1xuICAgIHRoaXMuY2RyLmRldGVjdENoYW5nZXMoKTtcbiAgfVxuXG4gIG1vdmVJY29uKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmxpc3RPZkljb25FbGVtZW50ICYmIHRoaXMubGlzdE9mSWNvbkVsZW1lbnQubGVuZ3RoKSB7XG4gICAgICBjb25zdCBmaXJzdENoaWxkRWxlbWVudCA9IGZpbmRGaXJzdE5vdEVtcHR5Tm9kZSh0aGlzLmNvbnRlbnRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQpO1xuICAgICAgY29uc3QgbGFzdENoaWxkRWxlbWVudCA9IGZpbmRMYXN0Tm90RW1wdHlOb2RlKHRoaXMuY29udGVudEVsZW1lbnQubmF0aXZlRWxlbWVudCk7XG4gICAgICBpZiAoZmlyc3RDaGlsZEVsZW1lbnQgJiYgZmlyc3RDaGlsZEVsZW1lbnQgPT09IHRoaXMubGlzdE9mSWNvbkVsZW1lbnQuZmlyc3QubmF0aXZlRWxlbWVudCkge1xuICAgICAgICB0aGlzLnJlbmRlcmVyLmluc2VydEJlZm9yZSh0aGlzLmVsLCBmaXJzdENoaWxkRWxlbWVudCwgdGhpcy5jb250ZW50RWxlbWVudC5uYXRpdmVFbGVtZW50KTtcbiAgICAgICAgdGhpcy5pY29uRWxlbWVudCA9IGZpcnN0Q2hpbGRFbGVtZW50IGFzIEhUTUxFbGVtZW50O1xuICAgICAgfSBlbHNlIGlmIChsYXN0Q2hpbGRFbGVtZW50ICYmIGxhc3RDaGlsZEVsZW1lbnQgPT09IHRoaXMubGlzdE9mSWNvbkVsZW1lbnQubGFzdC5uYXRpdmVFbGVtZW50KSB7XG4gICAgICAgIHRoaXMucmVuZGVyZXIuYXBwZW5kQ2hpbGQodGhpcy5lbCwgbGFzdENoaWxkRWxlbWVudCk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgY29uc3RydWN0b3IoXG4gICAgcHJpdmF0ZSBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHByaXZhdGUgY2RyOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjIsXG4gICAgcHJpdmF0ZSBuelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2U6IE56VXBkYXRlSG9zdENsYXNzU2VydmljZSxcbiAgICBwcml2YXRlIG5nWm9uZTogTmdab25lLFxuICAgIEBPcHRpb25hbCgpIEBJbmplY3QoTlpfV0FWRV9HTE9CQUxfQ09ORklHKSBwcml2YXRlIHdhdmVDb25maWc6IE56V2F2ZUNvbmZpZyxcbiAgICBAT3B0aW9uYWwoKSBASW5qZWN0KEFOSU1BVElPTl9NT0RVTEVfVFlQRSkgcHJpdmF0ZSBhbmltYXRpb25UeXBlOiBzdHJpbmdcbiAgKSB7fVxuXG4gIG5nQWZ0ZXJDb250ZW50SW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLmNoZWNrQ29udGVudCgpO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5zZXRDbGFzc01hcCgpO1xuICAgIHRoaXMubnpXYXZlLm5nT25Jbml0KCk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLm56V2F2ZS5uZ09uRGVzdHJveSgpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChcbiAgICAgIGNoYW5nZXMubnpCbG9jayB8fFxuICAgICAgY2hhbmdlcy5uekdob3N0IHx8XG4gICAgICBjaGFuZ2VzLm56U2VhcmNoIHx8XG4gICAgICBjaGFuZ2VzLm56VHlwZSB8fFxuICAgICAgY2hhbmdlcy5uelNoYXBlIHx8XG4gICAgICBjaGFuZ2VzLm56U2l6ZSB8fFxuICAgICAgY2hhbmdlcy5uekxvYWRpbmdcbiAgICApIHtcbiAgICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgICB9XG4gICAgaWYgKGNoYW5nZXMubnpMb2FkaW5nKSB7XG4gICAgICB0aGlzLnVwZGF0ZUljb25EaXNwbGF5KHRoaXMubnpMb2FkaW5nKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==