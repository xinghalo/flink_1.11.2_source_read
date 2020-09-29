/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { slideMotion } from '../core/animation/slide';
import { DateHelperService } from '../i18n/date-helper.service';
export class NzPickerComponent {
    /**
     * @param {?} dateHelper
     * @param {?} changeDetector
     */
    constructor(dateHelper, changeDetector) {
        this.dateHelper = dateHelper;
        this.changeDetector = changeDetector;
        this.noAnimation = false;
        this.isRange = false;
        this.open = undefined;
        this.valueChange = new EventEmitter();
        this.openChange = new EventEmitter(); // Emitted when overlay's open state change
        this.prefixCls = 'ant-calendar';
        this.animationOpenState = false;
        this.overlayOpen = false; // Available when "open"=undefined
        // Available when "open"=undefined
        this.overlayOffsetY = 0;
        this.overlayOffsetX = -2;
        this.overlayPositions = (/** @type {?} */ ([
            {
                // offsetX: -10, // TODO: What a pity, cdk/overlay current not support offset configs even though it already provide these properties
                // offsetY: -10,
                originX: 'start',
                originY: 'top',
                overlayX: 'start',
                overlayY: 'top'
            },
            {
                originX: 'start',
                originY: 'bottom',
                overlayX: 'start',
                overlayY: 'bottom'
            },
            {
                originX: 'end',
                originY: 'top',
                overlayX: 'end',
                overlayY: 'top'
            },
            {
                originX: 'end',
                originY: 'bottom',
                overlayX: 'end',
                overlayY: 'bottom'
            }
        ]));
        this.dropdownAnimation = 'bottom';
        this.currentPositionX = 'start';
        this.currentPositionY = 'top';
    }
    /**
     * @return {?}
     */
    get realOpenState() {
        // The value that really decide the open state of overlay
        return this.isOpenHandledByUser() ? !!this.open : this.overlayOpen;
    }
    /**
     * @return {?}
     */
    ngOnInit() { }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        if (this.autoFocus) {
            if (this.isRange) {
                /** @type {?} */
                const firstInput = (/** @type {?} */ (((/** @type {?} */ (this.pickerInput.nativeElement))).querySelector('input:first-child')));
                firstInput.focus(); // Focus on the first input
            }
            else {
                this.pickerInput.nativeElement.focus();
            }
        }
    }
    // Show overlay content
    /**
     * @return {?}
     */
    showOverlay() {
        if (!this.realOpenState) {
            this.overlayOpen = true;
            this.openChange.emit(this.overlayOpen);
            setTimeout((/**
             * @return {?}
             */
            () => {
                if (this.cdkConnectedOverlay && this.cdkConnectedOverlay.overlayRef) {
                    this.cdkConnectedOverlay.overlayRef.updatePosition();
                }
            }));
        }
    }
    /**
     * @return {?}
     */
    hideOverlay() {
        if (this.realOpenState) {
            this.overlayOpen = false;
            this.openChange.emit(this.overlayOpen);
        }
    }
    /**
     * @return {?}
     */
    onClickInputBox() {
        if (!this.disabled && !this.isOpenHandledByUser()) {
            this.showOverlay();
        }
    }
    /**
     * @return {?}
     */
    onClickBackdrop() {
        this.hideOverlay();
    }
    /**
     * @return {?}
     */
    onOverlayDetach() {
        this.hideOverlay();
    }
    // NOTE: A issue here, the first time position change, the animation will not be triggered.
    // Because the overlay's "positionChange" event is emitted after the content's full shown up.
    // All other components like "nz-dropdown" which depends on overlay also has the same issue.
    // See: https://github.com/NG-ZORRO/ng-zorro-antd/issues/1429
    /**
     * @param {?} position
     * @return {?}
     */
    onPositionChange(position) {
        this.dropdownAnimation = position.connectionPair.originY === 'top' ? 'bottom' : 'top';
        this.currentPositionX = (/** @type {?} */ (position.connectionPair.originX));
        this.currentPositionY = (/** @type {?} */ (position.connectionPair.originY));
        this.changeDetector.detectChanges(); // Take side-effects to position styles
    }
    /**
     * @param {?} event
     * @return {?}
     */
    onClickClear(event) {
        event.preventDefault();
        event.stopPropagation();
        this.value = this.isRange ? [] : null;
        this.valueChange.emit(this.value);
    }
    /**
     * @param {?=} partType
     * @return {?}
     */
    getReadableValue(partType) {
        /** @type {?} */
        let value;
        if (this.isRange) {
            value = ((/** @type {?} */ (this.value)))[this.getPartTypeIndex((/** @type {?} */ (partType)))];
        }
        else {
            value = (/** @type {?} */ (this.value));
        }
        return value ? this.dateHelper.format(value.nativeDate, this.format) : null;
    }
    /**
     * @param {?} partType
     * @return {?}
     */
    getPartTypeIndex(partType) {
        return { left: 0, right: 1 }[partType];
    }
    /**
     * @param {?=} partType
     * @return {?}
     */
    getPlaceholder(partType) {
        return this.isRange ? this.placeholder[this.getPartTypeIndex((/** @type {?} */ (partType)))] : ((/** @type {?} */ (this.placeholder)));
    }
    /**
     * @param {?} value
     * @return {?}
     */
    isEmptyValue(value) {
        if (value === null) {
            return true;
        }
        else if (this.isRange) {
            return !value || !Array.isArray(value) || value.every((/**
             * @param {?} val
             * @return {?}
             */
            val => !val));
        }
        else {
            return !value;
        }
    }
    // Whether open state is permanently controlled by user himself
    /**
     * @return {?}
     */
    isOpenHandledByUser() {
        return this.open !== undefined;
    }
    /**
     * @return {?}
     */
    animationStart() {
        if (this.realOpenState) {
            this.animationOpenState = true;
        }
    }
    /**
     * @return {?}
     */
    animationDone() {
        this.animationOpenState = this.realOpenState;
    }
}
NzPickerComponent.decorators = [
    { type: Component, args: [{
                encapsulation: ViewEncapsulation.None,
                selector: 'nz-picker',
                template: "<span\n  cdkOverlayOrigin\n  #origin=\"cdkOverlayOrigin\"\n  class=\"{{ prefixCls }}-picker {{ size ? prefixCls + '-picker-' + size : '' }} {{ className }}\"\n  [ngStyle]=\"style\"\n  tabindex=\"0\"\n  (click)=\"onClickInputBox()\"\n>\n  <!-- Content of single picker -->\n  <ng-container *ngIf=\"!isRange\">\n    <input\n      #pickerInput\n      class=\"{{ prefixCls }}-picker-input ant-input\"\n      [class.ant-input-lg]=\"size === 'large'\"\n      [class.ant-input-sm]=\"size === 'small'\"\n      [class.ant-input-disabled]=\"disabled\"\n\n      [disabled]=\"disabled\"\n      readonly\n      value=\"{{ getReadableValue() }}\"\n      placeholder=\"{{ getPlaceholder() }}\"\n    />\n    <ng-container *ngTemplateOutlet=\"tplRightRest\"></ng-container>\n  </ng-container>\n\n  <!-- Content of range picker -->\n  <ng-container *ngIf=\"isRange\">\n    <span\n      #pickerInput\n      class=\"{{ prefixCls }}-picker-input ant-input\"\n      [class.ant-input-lg]=\"size === 'large'\"\n      [class.ant-input-sm]=\"size === 'small'\"\n      [class.ant-input-disabled]=\"disabled\"\n    >\n      <ng-container *ngTemplateOutlet=\"tplRangeInput; context: { partType: 'left' }\"></ng-container>\n      <span class=\"{{ prefixCls }}-range-picker-separator\"> ~ </span>\n      <ng-container *ngTemplateOutlet=\"tplRangeInput; context: { partType: 'right' }\"></ng-container>\n      <ng-container *ngTemplateOutlet=\"tplRightRest\"></ng-container>\n    </span>\n  </ng-container>\n</span>\n\n<!-- Input for Range ONLY -->\n<ng-template #tplRangeInput let-partType=\"partType\">\n  <input\n    class=\"{{ prefixCls }}-range-picker-input\"\n    [disabled]=\"disabled\"\n    readonly\n    value=\"{{ getReadableValue(partType) }}\"\n    placeholder=\"{{ getPlaceholder(partType) }}\"\n  />\n</ng-template>\n\n<!-- Right operator icons -->\n<ng-template #tplRightRest>\n  <i\n    nz-icon\n    type=\"close-circle\"\n    theme=\"fill\"\n    *ngIf=\"!disabled && !isEmptyValue(value) && allowClear\"\n    class=\"{{ prefixCls }}-picker-clear\"\n    (click)=\"onClickClear($event)\"\n  ></i>\n  <span class=\"{{ prefixCls }}-picker-icon\">\n    <i nz-icon type=\"calendar\"></i>\n  </span>\n</ng-template>\n\n<!-- Overlay -->\n<ng-template\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  [cdkConnectedOverlayOrigin]=\"origin\"\n  [cdkConnectedOverlayOpen]=\"realOpenState\"\n  [cdkConnectedOverlayHasBackdrop]=\"!isOpenHandledByUser()\"\n  [cdkConnectedOverlayPositions]=\"overlayPositions\"\n  (positionChange)=\"onPositionChange($event)\"\n  (backdropClick)=\"onClickBackdrop()\"\n  (detach)=\"onOverlayDetach()\"\n>\n  <div\n    [nzNoAnimation]=\"noAnimation\"\n    [@slideMotion]=\"dropdownAnimation\"\n    (@slideMotion.start)=\"animationStart()\"\n    (@slideMotion.done)=\"animationDone()\"\n    style=\"position: relative;\"\n    [style.left]=\"currentPositionX === 'start' ? '-2px' : '2px'\"\n    [style.top]=\"currentPositionY === 'top' ? '-2px' : '2px'\"\n  > <!-- Compatible for overlay that not support offset dynamically and immediately -->\n    <ng-content></ng-content>\n  </div>\n</ng-template>",
                animations: [slideMotion],
                changeDetection: ChangeDetectionStrategy.OnPush
            }] }
];
/** @nocollapse */
NzPickerComponent.ctorParameters = () => [
    { type: DateHelperService },
    { type: ChangeDetectorRef }
];
NzPickerComponent.propDecorators = {
    noAnimation: [{ type: Input }],
    isRange: [{ type: Input }],
    open: [{ type: Input }],
    disabled: [{ type: Input }],
    placeholder: [{ type: Input }],
    allowClear: [{ type: Input }],
    autoFocus: [{ type: Input }],
    className: [{ type: Input }],
    format: [{ type: Input }],
    size: [{ type: Input }],
    style: [{ type: Input }],
    value: [{ type: Input }],
    valueChange: [{ type: Output }],
    openChange: [{ type: Output }],
    origin: [{ type: ViewChild, args: ['origin',] }],
    cdkConnectedOverlay: [{ type: ViewChild, args: [CdkConnectedOverlay,] }],
    pickerInput: [{ type: ViewChild, args: ['pickerInput',] }]
};
if (false) {
    /** @type {?} */
    NzPickerComponent.prototype.noAnimation;
    /** @type {?} */
    NzPickerComponent.prototype.isRange;
    /** @type {?} */
    NzPickerComponent.prototype.open;
    /** @type {?} */
    NzPickerComponent.prototype.disabled;
    /** @type {?} */
    NzPickerComponent.prototype.placeholder;
    /** @type {?} */
    NzPickerComponent.prototype.allowClear;
    /** @type {?} */
    NzPickerComponent.prototype.autoFocus;
    /** @type {?} */
    NzPickerComponent.prototype.className;
    /** @type {?} */
    NzPickerComponent.prototype.format;
    /** @type {?} */
    NzPickerComponent.prototype.size;
    /** @type {?} */
    NzPickerComponent.prototype.style;
    /** @type {?} */
    NzPickerComponent.prototype.value;
    /** @type {?} */
    NzPickerComponent.prototype.valueChange;
    /** @type {?} */
    NzPickerComponent.prototype.openChange;
    /** @type {?} */
    NzPickerComponent.prototype.origin;
    /** @type {?} */
    NzPickerComponent.prototype.cdkConnectedOverlay;
    /** @type {?} */
    NzPickerComponent.prototype.pickerInput;
    /** @type {?} */
    NzPickerComponent.prototype.prefixCls;
    /** @type {?} */
    NzPickerComponent.prototype.animationOpenState;
    /** @type {?} */
    NzPickerComponent.prototype.overlayOpen;
    /** @type {?} */
    NzPickerComponent.prototype.overlayOffsetY;
    /** @type {?} */
    NzPickerComponent.prototype.overlayOffsetX;
    /** @type {?} */
    NzPickerComponent.prototype.overlayPositions;
    /** @type {?} */
    NzPickerComponent.prototype.dropdownAnimation;
    /** @type {?} */
    NzPickerComponent.prototype.currentPositionX;
    /** @type {?} */
    NzPickerComponent.prototype.currentPositionY;
    /**
     * @type {?}
     * @private
     */
    NzPickerComponent.prototype.dateHelper;
    /**
     * @type {?}
     * @private
     */
    NzPickerComponent.prototype.changeDetector;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlja2VyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9waWNrZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsbUJBQW1CLEVBQ25CLGdCQUFnQixFQUdqQixNQUFNLHNCQUFzQixDQUFDO0FBQzlCLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLEtBQUssRUFFTCxNQUFNLEVBQ04sU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsV0FBVyxFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFDdEQsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sNkJBQTZCLENBQUM7QUFVaEUsTUFBTSxPQUFPLGlCQUFpQjs7Ozs7SUE4RDVCLFlBQW9CLFVBQTZCLEVBQVUsY0FBaUM7UUFBeEUsZUFBVSxHQUFWLFVBQVUsQ0FBbUI7UUFBVSxtQkFBYyxHQUFkLGNBQWMsQ0FBbUI7UUE3RG5GLGdCQUFXLEdBQVksS0FBSyxDQUFDO1FBQzdCLFlBQU8sR0FBWSxLQUFLLENBQUM7UUFDekIsU0FBSSxHQUF3QixTQUFTLENBQUM7UUFVNUIsZ0JBQVcsR0FBRyxJQUFJLFlBQVksRUFBa0MsQ0FBQztRQUNqRSxlQUFVLEdBQUcsSUFBSSxZQUFZLEVBQVcsQ0FBQyxDQUFDLDJDQUEyQztRQU14RyxjQUFTLEdBQUcsY0FBYyxDQUFDO1FBQzNCLHVCQUFrQixHQUFHLEtBQUssQ0FBQztRQUMzQixnQkFBVyxHQUFZLEtBQUssQ0FBQyxDQUFDLGtDQUFrQzs7UUFDaEUsbUJBQWMsR0FBVyxDQUFDLENBQUM7UUFDM0IsbUJBQWMsR0FBVyxDQUFDLENBQUMsQ0FBQztRQUM1QixxQkFBZ0IsR0FBNkIsbUJBQUE7WUFDM0M7OztnQkFHRSxPQUFP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QVUsRUFBRSxDQUFDLFdBQVcsQ0FBQztnQkFDekIsZUFBZSxFQUFFLHVCQUF1QixDQUFDLE1BQU07YUFDaEQ7Ozs7WUFUUSxpQkFBaUI7WUFaeEIsaUJBQWlCOzs7MEJBdUJoQixLQUFLO3NCQUNMLEtBQUs7bUJBQ0wsS0FBSzt1QkFDTCxLQUFLOzBCQUNMLEtBQUs7eUJBQ0wsS0FBSzt3QkFDTCxLQUFLO3dCQUNMLEtBQUs7cUJBQ0wsS0FBSzttQkFDTCxLQUFLO29CQUNMLEtBQUs7b0JBQ0wsS0FBSzswQkFDTCxNQUFNO3lCQUNOLE1BQU07cUJBRU4sU0FBUyxTQUFDLFFBQVE7a0NBQ2xCLFNBQVMsU0FBQyxtQkFBbUI7MEJBQzdCLFNBQVMsU0FBQyxhQUFhOzs7O0lBakJ4Qix3Q0FBc0M7O0lBQ3RDLG9DQUFrQzs7SUFDbEMsaUNBQStDOztJQUMvQyxxQ0FBMkI7O0lBQzNCLHdDQUF3Qzs7SUFDeEMsdUNBQTZCOztJQUM3QixzQ0FBNEI7O0lBQzVCLHNDQUEyQjs7SUFDM0IsbUNBQXdCOztJQUN4QixpQ0FBaUM7O0lBQ2pDLGtDQUF1Qjs7SUFDdkIsa0NBQStDOztJQUMvQyx3Q0FBb0Y7O0lBQ3BGLHVDQUE0RDs7SUFFNUQsbUNBQThDOztJQUM5QyxnREFBeUU7O0lBQ3pFLHdDQUFrRDs7SUFFbEQsc0NBQTJCOztJQUMzQiwrQ0FBMkI7O0lBQzNCLHdDQUE2Qjs7SUFDN0IsMkNBQTJCOztJQUMzQiwyQ0FBNEI7O0lBQzVCLDZDQTJCOEI7O0lBQzlCLDhDQUErQzs7SUFDL0MsNkNBQTRDOztJQUM1Qyw2Q0FBMkM7Ozs7O0lBTy9CLHVDQUFxQzs7Ozs7SUFBRSwyQ0FBeUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDZGtDb25uZWN0ZWRPdmVybGF5LFxuICBDZGtPdmVybGF5T3JpZ2luLFxuICBDb25uZWN0ZWRPdmVybGF5UG9zaXRpb25DaGFuZ2UsXG4gIENvbm5lY3Rpb25Qb3NpdGlvblBhaXJcbn0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHtcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSW5wdXQsXG4gIE9uSW5pdCxcbiAgT3V0cHV0LFxuICBWaWV3Q2hpbGQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBzbGlkZU1vdGlvbiB9IGZyb20gJy4uL2NvcmUvYW5pbWF0aW9uL3NsaWRlJztcbmltcG9ydCB7IERhdGVIZWxwZXJTZXJ2aWNlIH0gZnJvbSAnLi4vaTE4bi9kYXRlLWhlbHBlci5zZXJ2aWNlJztcbmltcG9ydCB7IENhbmR5RGF0ZSB9IGZyb20gJy4vbGliL2NhbmR5LWRhdGUnO1xuXG5AQ29tcG9uZW50KHtcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZSxcbiAgc2VsZWN0b3I6ICduei1waWNrZXInLFxuICB0ZW1wbGF0ZVVybDogJy4vcGlja2VyLmNvbXBvbmVudC5odG1sJyxcbiAgYW5pbWF0aW9uczogW3NsaWRlTW90aW9uXSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2hcbn0pXG5leHBvcnQgY2xhc3MgTnpQaWNrZXJDb21wb25lbnQgaW1wbGVtZW50cyBPbkluaXQsIEFmdGVyVmlld0luaXQge1xuICBASW5wdXQoKSBub0FuaW1hdGlvbjogYm9vbGVhbiA9IGZhbHNlO1xuICBASW5wdXQoKSBpc1JhbmdlOiBib29sZWFuID0gZmFsc2U7XG4gIEBJbnB1dCgpIG9wZW46IGJvb2xlYW4gfCB1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gIEBJbnB1dCgpIGRpc2FibGVkOiBib29sZWFuO1xuICBASW5wdXQoKSBwbGFjZWhvbGRlcjogc3RyaW5nIHwgc3RyaW5nW107XG4gIEBJbnB1dCgpIGFsbG93Q2xlYXI6IGJvb2xlYW47XG4gIEBJbnB1dCgpIGF1dG9Gb2N1czogYm9vbGVhbjtcbiAgQElucHV0KCkgY2xhc3NOYW1lOiBzdHJpbmc7XG4gIEBJbnB1dCgpIGZvcm1hdDogc3RyaW5nO1xuICBASW5wdXQoKSBzaXplOiAnbGFyZ2UnIHwgJ3NtYWxsJztcbiAgQElucHV0KCkgc3R5bGU6IG9iamVjdDtcbiAgQElucHV0KCkgdmFsdWU6IENhbmR5RGF0ZSB8IENhbmR5RGF0ZVtdIHwgbnVsbDtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IHZhbHVlQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxDYW5keURhdGUgfCBDYW5keURhdGVbXSB8IG51bGw+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBvcGVuQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpOyAvLyBFbWl0dGVkIHdoZW4gb3ZlcmxheSdzIG9wZW4gc3RhdGUgY2hhbmdlXG5cbiAgQFZpZXdDaGlsZCgnb3JpZ2luJykgb3JpZ2luOiBDZGtPdmVybGF5T3JpZ2luO1xuICBAVmlld0NoaWxkKENka0Nvbm5lY3RlZE92ZXJsYXkpIGNka0Nvbm5lY3RlZE92ZXJsYXk6IENka0Nvbm5lY3RlZE92ZXJsYXk7XG4gIEBWaWV3Q2hpbGQoJ3BpY2tlcklucHV0JykgcGlja2VySW5wdXQ6IEVsZW1lbnRSZWY7XG5cbiAgcHJlZml4Q2xzID0gJ2FudC1jYWxlbmRhcic7XG4gIGFuaW1hdGlvbk9wZW5TdGF0ZSA9IGZhbHNlO1xuICBvdmVybGF5T3BlbjogYm9vbGVhbiA9IGZhbHNlOyAvLyBBdmFpbGFibGUgd2hlbiBcIm9wZW5cIj11bmRlZmluZWRcbiAgb3ZlcmxheU9mZnNldFk6IG51bWJlciA9IDA7XG4gIG92ZXJsYXlPZmZzZXRYOiBudW1iZXIgPSAtMjtcbiAgb3ZlcmxheVBvc2l0aW9uczogQ29ubmVjdGlvblBvc2l0aW9uUGFpcltdID0gW1xuICAgIHtcbiAgICAgIC8vIG9mZnNldFg6IC0xMCwgLy8gVE9ETzogV2hhdCBhIHBpdHksIGNkay9vdmVybGF5IGN1cnJlbnQgbm90IHN1cHBvcnQgb2Zmc2V0IGNvbmZpZ3MgZXZlbiB0aG91Z2ggaXQgYWxyZWFkeSBwcm92aWRlIHRoZXNlIHByb3BlcnRpZXNcbiAgICAgIC8vIG9mZnNldFk6IC0xMCxcbiAgICAgIG9yaWdpblg6ICdzdGFydCcsXG4gICAgICBvcmlnaW5ZOiAndG9wJyxcbiAgICAgIG92ZXJsYXlYOiAnc3RhcnQnLFxuICAgICAgb3ZlcmxheVk6ICd0b3AnXG4gICAgfSxcbiAgICB7XG4gICAgICBvcmlnaW5YOiAnc3RhcnQnLFxuICAgICAgb3JpZ2luWTogJ2JvdHRvbScsXG4gICAgICBvdmVybGF5WDogJ3N0YXJ0JyxcbiAgICAgIG92ZXJsYXlZOiAnYm90dG9tJ1xuICAgIH0sXG4gICAge1xuICAgICAgb3JpZ2luWDogJ2VuZCcsXG4gICAgICBvcmlnaW5ZOiAndG9wJyxcbiAgICAgIG92ZXJsYXlYOiAnZW5kJyxcbiAgICAgIG92ZXJsYXlZOiAndG9wJ1xuICAgIH0sXG4gICAge1xuICAgICAgb3JpZ2luWDogJ2VuZCcsXG4gICAgICBvcmlnaW5ZOiAnYm90dG9tJyxcbiAgICAgIG92ZXJsYXlYOiAnZW5kJyxcbiAgICAgIG92ZXJsYXlZOiAnYm90dG9tJ1xuICAgIH1cbiAgXSBhcyBDb25uZWN0aW9uUG9zaXRpb25QYWlyW107XG4gIGRyb3Bkb3duQW5pbWF0aW9uOiAndG9wJyB8ICdib3R0b20nID0gJ2JvdHRvbSc7XG4gIGN1cnJlbnRQb3NpdGlvblg6ICdzdGFydCcgfCAnZW5kJyA9ICdzdGFydCc7XG4gIGN1cnJlbnRQb3NpdGlvblk6ICd0b3AnIHwgJ2JvdHRvbScgPSAndG9wJztcblxuICBnZXQgcmVhbE9wZW5TdGF0ZSgpOiBib29sZWFuIHtcbiAgICAvLyBUaGUgdmFsdWUgdGhhdCByZWFsbHkgZGVjaWRlIHRoZSBvcGVuIHN0YXRlIG9mIG92ZXJsYXlcbiAgICByZXR1cm4gdGhpcy5pc09wZW5IYW5kbGVkQnlVc2VyKCkgPyAhIXRoaXMub3BlbiA6IHRoaXMub3ZlcmxheU9wZW47XG4gIH1cblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGRhdGVIZWxwZXI6IERhdGVIZWxwZXJTZXJ2aWNlLCBwcml2YXRlIGNoYW5nZURldGVjdG9yOiBDaGFuZ2VEZXRlY3RvclJlZikge31cblxuICBuZ09uSW5pdCgpOiB2b2lkIHt9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmF1dG9Gb2N1cykge1xuICAgICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgICBjb25zdCBmaXJzdElucHV0ID0gKHRoaXMucGlja2VySW5wdXQubmF0aXZlRWxlbWVudCBhcyBIVE1MRWxlbWVudCkucXVlcnlTZWxlY3RvcihcbiAgICAgICAgICAnaW5wdXQ6Zmlyc3QtY2hpbGQnXG4gICAgICAgICkgYXMgSFRNTElucHV0RWxlbWVudDtcbiAgICAgICAgZmlyc3RJbnB1dC5mb2N1cygpOyAvLyBGb2N1cyBvbiB0aGUgZmlyc3QgaW5wdXRcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMucGlja2VySW5wdXQubmF0aXZlRWxlbWVudC5mb2N1cygpO1xuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIC8vIFNob3cgb3ZlcmxheSBjb250ZW50XG4gIHNob3dPdmVybGF5KCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5yZWFsT3BlblN0YXRlKSB7XG4gICAgICB0aGlzLm92ZXJsYXlPcGVuID0gdHJ1ZTtcbiAgICAgIHRoaXMub3BlbkNoYW5nZS5lbWl0KHRoaXMub3ZlcmxheU9wZW4pO1xuICAgICAgc2V0VGltZW91dCgoKSA9PiB7XG4gICAgICAgIGlmICh0aGlzLmNka0Nvbm5lY3RlZE92ZXJsYXkgJiYgdGhpcy5jZGtDb25uZWN0ZWRPdmVybGF5Lm92ZXJsYXlSZWYpIHtcbiAgICAgICAgICB0aGlzLmNka0Nvbm5lY3RlZE92ZXJsYXkub3ZlcmxheVJlZi51cGRhdGVQb3NpdGlvbigpO1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH1cblxuICBoaWRlT3ZlcmxheSgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5yZWFsT3BlblN0YXRlKSB7XG4gICAgICB0aGlzLm92ZXJsYXlPcGVuID0gZmFsc2U7XG4gICAgICB0aGlzLm9wZW5DaGFuZ2UuZW1pdCh0aGlzLm92ZXJsYXlPcGVuKTtcbiAgICB9XG4gIH1cblxuICBvbkNsaWNrSW5wdXRCb3goKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLmRpc2FibGVkICYmICF0aGlzLmlzT3BlbkhhbmRsZWRCeVVzZXIoKSkge1xuICAgICAgdGhpcy5zaG93T3ZlcmxheSgpO1xuICAgIH1cbiAgfVxuXG4gIG9uQ2xpY2tCYWNrZHJvcCgpOiB2b2lkIHtcbiAgICB0aGlzLmhpZGVPdmVybGF5KCk7XG4gIH1cblxuICBvbk92ZXJsYXlEZXRhY2goKTogdm9pZCB7XG4gICAgdGhpcy5oaWRlT3ZlcmxheSgpO1xuICB9XG5cbiAgLy8gTk9URTogQSBpc3N1ZSBoZXJlLCB0aGUgZmlyc3QgdGltZSBwb3NpdGlvbiBjaGFuZ2UsIHRoZSBhbmltYXRpb24gd2lsbCBub3QgYmUgdHJpZ2dlcmVkLlxuICAvLyBCZWNhdXNlIHRoZSBvdmVybGF5J3MgXCJwb3NpdGlvbkNoYW5nZVwiIGV2ZW50IGlzIGVtaXR0ZWQgYWZ0ZXIgdGhlIGNvbnRlbnQncyBmdWxsIHNob3duIHVwLlxuICAvLyBBbGwgb3RoZXIgY29tcG9uZW50cyBsaWtlIFwibnotZHJvcGRvd25cIiB3aGljaCBkZXBlbmRzIG9uIG92ZXJsYXkgYWxzbyBoYXMgdGhlIHNhbWUgaXNzdWUuXG4gIC8vIFNlZTogaHR0cHM6Ly9naXRodWIuY29tL05HLVpPUlJPL25nLXpvcnJvLWFudGQvaXNzdWVzLzE0MjlcbiAgb25Qb3NpdGlvbkNoYW5nZShwb3NpdGlvbjogQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9uQ2hhbmdlKTogdm9pZCB7XG4gICAgdGhpcy5kcm9wZG93bkFuaW1hdGlvbiA9IHBvc2l0aW9uLmNvbm5lY3Rpb25QYWlyLm9yaWdpblkgPT09ICd0b3AnID8gJ2JvdHRvbScgOiAndG9wJztcbiAgICB0aGlzLmN1cnJlbnRQb3NpdGlvblggPSBwb3NpdGlvbi5jb25uZWN0aW9uUGFpci5vcmlnaW5YIGFzICdzdGFydCcgfCAnZW5kJztcbiAgICB0aGlzLmN1cnJlbnRQb3NpdGlvblkgPSBwb3NpdGlvbi5jb25uZWN0aW9uUGFpci5vcmlnaW5ZIGFzICd0b3AnIHwgJ2JvdHRvbSc7XG4gICAgdGhpcy5jaGFuZ2VEZXRlY3Rvci5kZXRlY3RDaGFuZ2VzKCk7IC8vIFRha2Ugc2lkZS1lZmZlY3RzIHRvIHBvc2l0aW9uIHN0eWxlc1xuICB9XG5cbiAgb25DbGlja0NsZWFyKGV2ZW50OiBNb3VzZUV2ZW50KTogdm9pZCB7XG4gICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICBldmVudC5zdG9wUHJvcGFnYXRpb24oKTtcblxuICAgIHRoaXMudmFsdWUgPSB0aGlzLmlzUmFuZ2UgPyBbXSA6IG51bGw7XG4gICAgdGhpcy52YWx1ZUNoYW5nZS5lbWl0KHRoaXMudmFsdWUpO1xuICB9XG5cbiAgZ2V0UmVhZGFibGVWYWx1ZShwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiBzdHJpbmcgfCBudWxsIHtcbiAgICBsZXQgdmFsdWU6IENhbmR5RGF0ZTtcbiAgICBpZiAodGhpcy5pc1JhbmdlKSB7XG4gICAgICB2YWx1ZSA9ICh0aGlzLnZhbHVlIGFzIENhbmR5RGF0ZVtdKVt0aGlzLmdldFBhcnRUeXBlSW5kZXgocGFydFR5cGUgYXMgUmFuZ2VQYXJ0VHlwZSldO1xuICAgIH0gZWxzZSB7XG4gICAgICB2YWx1ZSA9IHRoaXMudmFsdWUgYXMgQ2FuZHlEYXRlO1xuICAgIH1cbiAgICByZXR1cm4gdmFsdWUgPyB0aGlzLmRhdGVIZWxwZXIuZm9ybWF0KHZhbHVlLm5hdGl2ZURhdGUsIHRoaXMuZm9ybWF0KSA6IG51bGw7XG4gIH1cblxuICBnZXRQYXJ0VHlwZUluZGV4KHBhcnRUeXBlOiBSYW5nZVBhcnRUeXBlKTogbnVtYmVyIHtcbiAgICByZXR1cm4geyBsZWZ0OiAwLCByaWdodDogMSB9W3BhcnRUeXBlXTtcbiAgfVxuXG4gIGdldFBsYWNlaG9sZGVyKHBhcnRUeXBlPzogUmFuZ2VQYXJ0VHlwZSk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMuaXNSYW5nZSA/IHRoaXMucGxhY2Vob2xkZXJbdGhpcy5nZXRQYXJ0VHlwZUluZGV4KHBhcnRUeXBlISldIDogKHRoaXMucGxhY2Vob2xkZXIgYXMgc3RyaW5nKTtcbiAgfVxuXG4gIGlzRW1wdHlWYWx1ZSh2YWx1ZTogQ2FuZHlEYXRlW10gfCBDYW5keURhdGUgfCBudWxsKTogYm9vbGVhbiB7XG4gICAgaWYgKHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gdHJ1ZTtcbiAgICB9IGVsc2UgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgcmV0dXJuICF2YWx1ZSB8fCAhQXJyYXkuaXNBcnJheSh2YWx1ZSkgfHwgdmFsdWUuZXZlcnkodmFsID0+ICF2YWwpO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gIXZhbHVlO1xuICAgIH1cbiAgfVxuXG4gIC8vIFdoZXRoZXIgb3BlbiBzdGF0ZSBpcyBwZXJtYW5lbnRseSBjb250cm9sbGVkIGJ5IHVzZXIgaGltc2VsZlxuICBpc09wZW5IYW5kbGVkQnlVc2VyKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLm9wZW4gIT09IHVuZGVmaW5lZDtcbiAgfVxuXG4gIGFuaW1hdGlvblN0YXJ0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnJlYWxPcGVuU3RhdGUpIHtcbiAgICAgIHRoaXMuYW5pbWF0aW9uT3BlblN0YXRlID0gdHJ1ZTtcbiAgICB9XG4gIH1cblxuICBhbmltYXRpb25Eb25lKCk6IHZvaWQge1xuICAgIHRoaXMuYW5pbWF0aW9uT3BlblN0YXRlID0gdGhpcy5yZWFsT3BlblN0YXRlO1xuICB9XG59XG5cbmV4cG9ydCB0eXBlIFJhbmdlUGFydFR5cGUgPSAnbGVmdCcgfCAncmlnaHQnO1xuIl19