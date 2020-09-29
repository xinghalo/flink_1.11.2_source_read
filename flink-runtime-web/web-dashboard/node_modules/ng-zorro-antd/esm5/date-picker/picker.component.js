/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { slideMotion } from '../core/animation/slide';
import { DateHelperService } from '../i18n/date-helper.service';
var NzPickerComponent = /** @class */ (function () {
    function NzPickerComponent(dateHelper, changeDetector) {
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
    Object.defineProperty(NzPickerComponent.prototype, "realOpenState", {
        get: /**
         * @return {?}
         */
        function () {
            // The value that really decide the open state of overlay
            return this.isOpenHandledByUser() ? !!this.open : this.overlayOpen;
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () { };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.ngAfterViewInit = /**
     * @return {?}
     */
    function () {
        if (this.autoFocus) {
            if (this.isRange) {
                /** @type {?} */
                var firstInput = (/** @type {?} */ (((/** @type {?} */ (this.pickerInput.nativeElement))).querySelector('input:first-child')));
                firstInput.focus(); // Focus on the first input
            }
            else {
                this.pickerInput.nativeElement.focus();
            }
        }
    };
    // Show overlay content
    // Show overlay content
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.showOverlay = 
    // Show overlay content
    /**
     * @return {?}
     */
    function () {
        var _this = this;
        if (!this.realOpenState) {
            this.overlayOpen = true;
            this.openChange.emit(this.overlayOpen);
            setTimeout((/**
             * @return {?}
             */
            function () {
                if (_this.cdkConnectedOverlay && _this.cdkConnectedOverlay.overlayRef) {
                    _this.cdkConnectedOverlay.overlayRef.updatePosition();
                }
            }));
        }
    };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.hideOverlay = /**
     * @return {?}
     */
    function () {
        if (this.realOpenState) {
            this.overlayOpen = false;
            this.openChange.emit(this.overlayOpen);
        }
    };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.onClickInputBox = /**
     * @return {?}
     */
    function () {
        if (!this.disabled && !this.isOpenHandledByUser()) {
            this.showOverlay();
        }
    };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.onClickBackdrop = /**
     * @return {?}
     */
    function () {
        this.hideOverlay();
    };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.onOverlayDetach = /**
     * @return {?}
     */
    function () {
        this.hideOverlay();
    };
    // NOTE: A issue here, the first time position change, the animation will not be triggered.
    // Because the overlay's "positionChange" event is emitted after the content's full shown up.
    // All other components like "nz-dropdown" which depends on overlay also has the same issue.
    // See: https://github.com/NG-ZORRO/ng-zorro-antd/issues/1429
    // NOTE: A issue here, the first time position change, the animation will not be triggered.
    // Because the overlay's "positionChange" event is emitted after the content's full shown up.
    // All other components like "nz-dropdown" which depends on overlay also has the same issue.
    // See: https://github.com/NG-ZORRO/ng-zorro-antd/issues/1429
    /**
     * @param {?} position
     * @return {?}
     */
    NzPickerComponent.prototype.onPositionChange = 
    // NOTE: A issue here, the first time position change, the animation will not be triggered.
    // Because the overlay's "positionChange" event is emitted after the content's full shown up.
    // All other components like "nz-dropdown" which depends on overlay also has the same issue.
    // See: https://github.com/NG-ZORRO/ng-zorro-antd/issues/1429
    /**
     * @param {?} position
     * @return {?}
     */
    function (position) {
        this.dropdownAnimation = position.connectionPair.originY === 'top' ? 'bottom' : 'top';
        this.currentPositionX = (/** @type {?} */ (position.connectionPair.originX));
        this.currentPositionY = (/** @type {?} */ (position.connectionPair.originY));
        this.changeDetector.detectChanges(); // Take side-effects to position styles
    };
    /**
     * @param {?} event
     * @return {?}
     */
    NzPickerComponent.prototype.onClickClear = /**
     * @param {?} event
     * @return {?}
     */
    function (event) {
        event.preventDefault();
        event.stopPropagation();
        this.value = this.isRange ? [] : null;
        this.valueChange.emit(this.value);
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    NzPickerComponent.prototype.getReadableValue = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        /** @type {?} */
        var value;
        if (this.isRange) {
            value = ((/** @type {?} */ (this.value)))[this.getPartTypeIndex((/** @type {?} */ (partType)))];
        }
        else {
            value = (/** @type {?} */ (this.value));
        }
        return value ? this.dateHelper.format(value.nativeDate, this.format) : null;
    };
    /**
     * @param {?} partType
     * @return {?}
     */
    NzPickerComponent.prototype.getPartTypeIndex = /**
     * @param {?} partType
     * @return {?}
     */
    function (partType) {
        return { left: 0, right: 1 }[partType];
    };
    /**
     * @param {?=} partType
     * @return {?}
     */
    NzPickerComponent.prototype.getPlaceholder = /**
     * @param {?=} partType
     * @return {?}
     */
    function (partType) {
        return this.isRange ? this.placeholder[this.getPartTypeIndex((/** @type {?} */ (partType)))] : ((/** @type {?} */ (this.placeholder)));
    };
    /**
     * @param {?} value
     * @return {?}
     */
    NzPickerComponent.prototype.isEmptyValue = /**
     * @param {?} value
     * @return {?}
     */
    function (value) {
        if (value === null) {
            return true;
        }
        else if (this.isRange) {
            return !value || !Array.isArray(value) || value.every((/**
             * @param {?} val
             * @return {?}
             */
            function (val) { return !val; }));
        }
        else {
            return !value;
        }
    };
    // Whether open state is permanently controlled by user himself
    // Whether open state is permanently controlled by user himself
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.isOpenHandledByUser = 
    // Whether open state is permanently controlled by user himself
    /**
     * @return {?}
     */
    function () {
        return this.open !== undefined;
    };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.animationStart = /**
     * @return {?}
     */
    function () {
        if (this.realOpenState) {
            this.animationOpenState = true;
        }
    };
    /**
     * @return {?}
     */
    NzPickerComponent.prototype.animationDone = /**
     * @return {?}
     */
    function () {
        this.animationOpenState = this.realOpenState;
    };
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
    NzPickerComponent.ctorParameters = function () { return [
        { type: DateHelperService },
        { type: ChangeDetectorRef }
    ]; };
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
    return NzPickerComponent;
}());
export { NzPickerComponent };
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoicGlja2VyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJkYXRlLXBpY2tlci9waWNrZXIuY29tcG9uZW50LnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsbUJBQW1CLEVBQ25CLGdCQUFnQixFQUdqQixNQUFNLHNCQUFzQixDQUFDO0FBQzlCLE9BQU8sRUFFTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLEtBQUssRUFFTCxNQUFNLEVBQ04sU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsV0FBVyxFQUFFLE1BQU0seUJBQXlCLENBQUM7QUFDdEQsT0FBTyxFQUFFLGlCQUFpQixFQUFFLE1BQU0sNkJBQTZCLENBQUM7QUFHaEU7SUFxRUUsMkJBQW9CLFVBQTZCLEVBQVUsY0FBaUM7UUFBeEUsZUFBVSxHQUFWLFVBQVUsQ0FBbUI7UUFBVSxtQkFBYyxHQUFkLGNBQWMsQ0FBbUI7UUE3RG5GLGdCQUFXLEdBQVksS0FBSyxDQUFDO1FBQzdCLFlBQU8sR0FBWSxLQUFLLENBQUM7UUFDekIsU0FBSSxHQUF3QixTQUFTLENBQUM7UUFVNUIsZ0JBQVcsR0FBRyxJQUFJLFlBQVksRUFBa0MsQ0FBQztRQUNqRSxlQUFVLEdBQUcsSUFBSSxZQUFZLEVBQVcsQ0FBQyxDQUFDLDJDQUEyQztRQU14RyxjQUFTLEdBQUcsY0FBYyxDQUFDO1FBQzNCLHVCQUFrQixHQUFHLEtBQUssQ0FBQztRQUMzQixnQkFBVyxHQUFZLEtBQUssQ0FBQyxDQUFDLGtDQUFrQzs7UUFDaEUsbUJBQWMsR0FBVyxDQUFDLENBQUM7UUFDM0IsbUJBQWMsR0FBVyxDQUFDLENBQUMsQ0FBQztRQUM1QixxQkFBZ0IsR0FBNkIsbUJBQUE7WUFDM0M7OztnQkFHRSxPQUFPLEVBQUUsT0FBTztnQkFDaEIsT0FB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OztJQUM1Qiw2Q0EyQjhCOztJQUM5Qiw4Q0FBK0M7O0lBQy9DLDZDQUE0Qzs7SUFDNUMsNkNBQTJDOzs7OztJQU8vQix1Q0FBcUM7Ozs7O0lBQUUsMkNBQXlDIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2RrQ29ubmVjdGVkT3ZlcmxheSxcbiAgQ2RrT3ZlcmxheU9yaWdpbixcbiAgQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9uQ2hhbmdlLFxuICBDb25uZWN0aW9uUG9zaXRpb25QYWlyXG59IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7XG4gIEFmdGVyVmlld0luaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPbkluaXQsXG4gIE91dHB1dCxcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgc2xpZGVNb3Rpb24gfSBmcm9tICcuLi9jb3JlL2FuaW1hdGlvbi9zbGlkZSc7XG5pbXBvcnQgeyBEYXRlSGVscGVyU2VydmljZSB9IGZyb20gJy4uL2kxOG4vZGF0ZS1oZWxwZXIuc2VydmljZSc7XG5pbXBvcnQgeyBDYW5keURhdGUgfSBmcm9tICcuL2xpYi9jYW5keS1kYXRlJztcblxuQENvbXBvbmVudCh7XG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotcGlja2VyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL3BpY2tlci5jb21wb25lbnQuaHRtbCcsXG4gIGFuaW1hdGlvbnM6IFtzbGlkZU1vdGlvbl0sXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoXG59KVxuZXhwb3J0IGNsYXNzIE56UGlja2VyQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBBZnRlclZpZXdJbml0IHtcbiAgQElucHV0KCkgbm9BbmltYXRpb246IGJvb2xlYW4gPSBmYWxzZTtcbiAgQElucHV0KCkgaXNSYW5nZTogYm9vbGVhbiA9IGZhbHNlO1xuICBASW5wdXQoKSBvcGVuOiBib29sZWFuIHwgdW5kZWZpbmVkID0gdW5kZWZpbmVkO1xuICBASW5wdXQoKSBkaXNhYmxlZDogYm9vbGVhbjtcbiAgQElucHV0KCkgcGxhY2Vob2xkZXI6IHN0cmluZyB8IHN0cmluZ1tdO1xuICBASW5wdXQoKSBhbGxvd0NsZWFyOiBib29sZWFuO1xuICBASW5wdXQoKSBhdXRvRm9jdXM6IGJvb2xlYW47XG4gIEBJbnB1dCgpIGNsYXNzTmFtZTogc3RyaW5nO1xuICBASW5wdXQoKSBmb3JtYXQ6IHN0cmluZztcbiAgQElucHV0KCkgc2l6ZTogJ2xhcmdlJyB8ICdzbWFsbCc7XG4gIEBJbnB1dCgpIHN0eWxlOiBvYmplY3Q7XG4gIEBJbnB1dCgpIHZhbHVlOiBDYW5keURhdGUgfCBDYW5keURhdGVbXSB8IG51bGw7XG4gIEBPdXRwdXQoKSByZWFkb25seSB2YWx1ZUNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Q2FuZHlEYXRlIHwgQ2FuZHlEYXRlW10gfCBudWxsPigpO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgb3BlbkNoYW5nZSA9IG5ldyBFdmVudEVtaXR0ZXI8Ym9vbGVhbj4oKTsgLy8gRW1pdHRlZCB3aGVuIG92ZXJsYXkncyBvcGVuIHN0YXRlIGNoYW5nZVxuXG4gIEBWaWV3Q2hpbGQoJ29yaWdpbicpIG9yaWdpbjogQ2RrT3ZlcmxheU9yaWdpbjtcbiAgQFZpZXdDaGlsZChDZGtDb25uZWN0ZWRPdmVybGF5KSBjZGtDb25uZWN0ZWRPdmVybGF5OiBDZGtDb25uZWN0ZWRPdmVybGF5O1xuICBAVmlld0NoaWxkKCdwaWNrZXJJbnB1dCcpIHBpY2tlcklucHV0OiBFbGVtZW50UmVmO1xuXG4gIHByZWZpeENscyA9ICdhbnQtY2FsZW5kYXInO1xuICBhbmltYXRpb25PcGVuU3RhdGUgPSBmYWxzZTtcbiAgb3ZlcmxheU9wZW46IGJvb2xlYW4gPSBmYWxzZTsgLy8gQXZhaWxhYmxlIHdoZW4gXCJvcGVuXCI9dW5kZWZpbmVkXG4gIG92ZXJsYXlPZmZzZXRZOiBudW1iZXIgPSAwO1xuICBvdmVybGF5T2Zmc2V0WDogbnVtYmVyID0gLTI7XG4gIG92ZXJsYXlQb3NpdGlvbnM6IENvbm5lY3Rpb25Qb3NpdGlvblBhaXJbXSA9IFtcbiAgICB7XG4gICAgICAvLyBvZmZzZXRYOiAtMTAsIC8vIFRPRE86IFdoYXQgYSBwaXR5LCBjZGsvb3ZlcmxheSBjdXJyZW50IG5vdCBzdXBwb3J0IG9mZnNldCBjb25maWdzIGV2ZW4gdGhvdWdoIGl0IGFscmVhZHkgcHJvdmlkZSB0aGVzZSBwcm9wZXJ0aWVzXG4gICAgICAvLyBvZmZzZXRZOiAtMTAsXG4gICAgICBvcmlnaW5YOiAnc3RhcnQnLFxuICAgICAgb3JpZ2luWTogJ3RvcCcsXG4gICAgICBvdmVybGF5WDogJ3N0YXJ0JyxcbiAgICAgIG92ZXJsYXlZOiAndG9wJ1xuICAgIH0sXG4gICAge1xuICAgICAgb3JpZ2luWDogJ3N0YXJ0JyxcbiAgICAgIG9yaWdpblk6ICdib3R0b20nLFxuICAgICAgb3ZlcmxheVg6ICdzdGFydCcsXG4gICAgICBvdmVybGF5WTogJ2JvdHRvbSdcbiAgICB9LFxuICAgIHtcbiAgICAgIG9yaWdpblg6ICdlbmQnLFxuICAgICAgb3JpZ2luWTogJ3RvcCcsXG4gICAgICBvdmVybGF5WDogJ2VuZCcsXG4gICAgICBvdmVybGF5WTogJ3RvcCdcbiAgICB9LFxuICAgIHtcbiAgICAgIG9yaWdpblg6ICdlbmQnLFxuICAgICAgb3JpZ2luWTogJ2JvdHRvbScsXG4gICAgICBvdmVybGF5WDogJ2VuZCcsXG4gICAgICBvdmVybGF5WTogJ2JvdHRvbSdcbiAgICB9XG4gIF0gYXMgQ29ubmVjdGlvblBvc2l0aW9uUGFpcltdO1xuICBkcm9wZG93bkFuaW1hdGlvbjogJ3RvcCcgfCAnYm90dG9tJyA9ICdib3R0b20nO1xuICBjdXJyZW50UG9zaXRpb25YOiAnc3RhcnQnIHwgJ2VuZCcgPSAnc3RhcnQnO1xuICBjdXJyZW50UG9zaXRpb25ZOiAndG9wJyB8ICdib3R0b20nID0gJ3RvcCc7XG5cbiAgZ2V0IHJlYWxPcGVuU3RhdGUoKTogYm9vbGVhbiB7XG4gICAgLy8gVGhlIHZhbHVlIHRoYXQgcmVhbGx5IGRlY2lkZSB0aGUgb3BlbiBzdGF0ZSBvZiBvdmVybGF5XG4gICAgcmV0dXJuIHRoaXMuaXNPcGVuSGFuZGxlZEJ5VXNlcigpID8gISF0aGlzLm9wZW4gOiB0aGlzLm92ZXJsYXlPcGVuO1xuICB9XG5cbiAgY29uc3RydWN0b3IocHJpdmF0ZSBkYXRlSGVscGVyOiBEYXRlSGVscGVyU2VydmljZSwgcHJpdmF0ZSBjaGFuZ2VEZXRlY3RvcjogQ2hhbmdlRGV0ZWN0b3JSZWYpIHt9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7fVxuXG4gIG5nQWZ0ZXJWaWV3SW5pdCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5hdXRvRm9jdXMpIHtcbiAgICAgIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgICAgY29uc3QgZmlyc3RJbnB1dCA9ICh0aGlzLnBpY2tlcklucHV0Lm5hdGl2ZUVsZW1lbnQgYXMgSFRNTEVsZW1lbnQpLnF1ZXJ5U2VsZWN0b3IoXG4gICAgICAgICAgJ2lucHV0OmZpcnN0LWNoaWxkJ1xuICAgICAgICApIGFzIEhUTUxJbnB1dEVsZW1lbnQ7XG4gICAgICAgIGZpcnN0SW5wdXQuZm9jdXMoKTsgLy8gRm9jdXMgb24gdGhlIGZpcnN0IGlucHV0XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLnBpY2tlcklucHV0Lm5hdGl2ZUVsZW1lbnQuZm9jdXMoKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICAvLyBTaG93IG92ZXJsYXkgY29udGVudFxuICBzaG93T3ZlcmxheSgpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMucmVhbE9wZW5TdGF0ZSkge1xuICAgICAgdGhpcy5vdmVybGF5T3BlbiA9IHRydWU7XG4gICAgICB0aGlzLm9wZW5DaGFuZ2UuZW1pdCh0aGlzLm92ZXJsYXlPcGVuKTtcbiAgICAgIHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgICBpZiAodGhpcy5jZGtDb25uZWN0ZWRPdmVybGF5ICYmIHRoaXMuY2RrQ29ubmVjdGVkT3ZlcmxheS5vdmVybGF5UmVmKSB7XG4gICAgICAgICAgdGhpcy5jZGtDb25uZWN0ZWRPdmVybGF5Lm92ZXJsYXlSZWYudXBkYXRlUG9zaXRpb24oKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgaGlkZU92ZXJsYXkoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMucmVhbE9wZW5TdGF0ZSkge1xuICAgICAgdGhpcy5vdmVybGF5T3BlbiA9IGZhbHNlO1xuICAgICAgdGhpcy5vcGVuQ2hhbmdlLmVtaXQodGhpcy5vdmVybGF5T3Blbik7XG4gICAgfVxuICB9XG5cbiAgb25DbGlja0lucHV0Qm94KCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5kaXNhYmxlZCAmJiAhdGhpcy5pc09wZW5IYW5kbGVkQnlVc2VyKCkpIHtcbiAgICAgIHRoaXMuc2hvd092ZXJsYXkoKTtcbiAgICB9XG4gIH1cblxuICBvbkNsaWNrQmFja2Ryb3AoKTogdm9pZCB7XG4gICAgdGhpcy5oaWRlT3ZlcmxheSgpO1xuICB9XG5cbiAgb25PdmVybGF5RGV0YWNoKCk6IHZvaWQge1xuICAgIHRoaXMuaGlkZU92ZXJsYXkoKTtcbiAgfVxuXG4gIC8vIE5PVEU6IEEgaXNzdWUgaGVyZSwgdGhlIGZpcnN0IHRpbWUgcG9zaXRpb24gY2hhbmdlLCB0aGUgYW5pbWF0aW9uIHdpbGwgbm90IGJlIHRyaWdnZXJlZC5cbiAgLy8gQmVjYXVzZSB0aGUgb3ZlcmxheSdzIFwicG9zaXRpb25DaGFuZ2VcIiBldmVudCBpcyBlbWl0dGVkIGFmdGVyIHRoZSBjb250ZW50J3MgZnVsbCBzaG93biB1cC5cbiAgLy8gQWxsIG90aGVyIGNvbXBvbmVudHMgbGlrZSBcIm56LWRyb3Bkb3duXCIgd2hpY2ggZGVwZW5kcyBvbiBvdmVybGF5IGFsc28gaGFzIHRoZSBzYW1lIGlzc3VlLlxuICAvLyBTZWU6IGh0dHBzOi8vZ2l0aHViLmNvbS9ORy1aT1JSTy9uZy16b3Jyby1hbnRkL2lzc3Vlcy8xNDI5XG4gIG9uUG9zaXRpb25DaGFuZ2UocG9zaXRpb246IENvbm5lY3RlZE92ZXJsYXlQb3NpdGlvbkNoYW5nZSk6IHZvaWQge1xuICAgIHRoaXMuZHJvcGRvd25BbmltYXRpb24gPSBwb3NpdGlvbi5jb25uZWN0aW9uUGFpci5vcmlnaW5ZID09PSAndG9wJyA/ICdib3R0b20nIDogJ3RvcCc7XG4gICAgdGhpcy5jdXJyZW50UG9zaXRpb25YID0gcG9zaXRpb24uY29ubmVjdGlvblBhaXIub3JpZ2luWCBhcyAnc3RhcnQnIHwgJ2VuZCc7XG4gICAgdGhpcy5jdXJyZW50UG9zaXRpb25ZID0gcG9zaXRpb24uY29ubmVjdGlvblBhaXIub3JpZ2luWSBhcyAndG9wJyB8ICdib3R0b20nO1xuICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3IuZGV0ZWN0Q2hhbmdlcygpOyAvLyBUYWtlIHNpZGUtZWZmZWN0cyB0byBwb3NpdGlvbiBzdHlsZXNcbiAgfVxuXG4gIG9uQ2xpY2tDbGVhcihldmVudDogTW91c2VFdmVudCk6IHZvaWQge1xuICAgIGV2ZW50LnByZXZlbnREZWZhdWx0KCk7XG4gICAgZXZlbnQuc3RvcFByb3BhZ2F0aW9uKCk7XG5cbiAgICB0aGlzLnZhbHVlID0gdGhpcy5pc1JhbmdlID8gW10gOiBudWxsO1xuICAgIHRoaXMudmFsdWVDaGFuZ2UuZW1pdCh0aGlzLnZhbHVlKTtcbiAgfVxuXG4gIGdldFJlYWRhYmxlVmFsdWUocGFydFR5cGU/OiBSYW5nZVBhcnRUeXBlKTogc3RyaW5nIHwgbnVsbCB7XG4gICAgbGV0IHZhbHVlOiBDYW5keURhdGU7XG4gICAgaWYgKHRoaXMuaXNSYW5nZSkge1xuICAgICAgdmFsdWUgPSAodGhpcy52YWx1ZSBhcyBDYW5keURhdGVbXSlbdGhpcy5nZXRQYXJ0VHlwZUluZGV4KHBhcnRUeXBlIGFzIFJhbmdlUGFydFR5cGUpXTtcbiAgICB9IGVsc2Uge1xuICAgICAgdmFsdWUgPSB0aGlzLnZhbHVlIGFzIENhbmR5RGF0ZTtcbiAgICB9XG4gICAgcmV0dXJuIHZhbHVlID8gdGhpcy5kYXRlSGVscGVyLmZvcm1hdCh2YWx1ZS5uYXRpdmVEYXRlLCB0aGlzLmZvcm1hdCkgOiBudWxsO1xuICB9XG5cbiAgZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZTogUmFuZ2VQYXJ0VHlwZSk6IG51bWJlciB7XG4gICAgcmV0dXJuIHsgbGVmdDogMCwgcmlnaHQ6IDEgfVtwYXJ0VHlwZV07XG4gIH1cblxuICBnZXRQbGFjZWhvbGRlcihwYXJ0VHlwZT86IFJhbmdlUGFydFR5cGUpOiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLmlzUmFuZ2UgPyB0aGlzLnBsYWNlaG9sZGVyW3RoaXMuZ2V0UGFydFR5cGVJbmRleChwYXJ0VHlwZSEpXSA6ICh0aGlzLnBsYWNlaG9sZGVyIGFzIHN0cmluZyk7XG4gIH1cblxuICBpc0VtcHR5VmFsdWUodmFsdWU6IENhbmR5RGF0ZVtdIHwgQ2FuZHlEYXRlIHwgbnVsbCk6IGJvb2xlYW4ge1xuICAgIGlmICh2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIHRydWU7XG4gICAgfSBlbHNlIGlmICh0aGlzLmlzUmFuZ2UpIHtcbiAgICAgIHJldHVybiAhdmFsdWUgfHwgIUFycmF5LmlzQXJyYXkodmFsdWUpIHx8IHZhbHVlLmV2ZXJ5KHZhbCA9PiAhdmFsKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuICF2YWx1ZTtcbiAgICB9XG4gIH1cblxuICAvLyBXaGV0aGVyIG9wZW4gc3RhdGUgaXMgcGVybWFuZW50bHkgY29udHJvbGxlZCBieSB1c2VyIGhpbXNlbGZcbiAgaXNPcGVuSGFuZGxlZEJ5VXNlcigpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5vcGVuICE9PSB1bmRlZmluZWQ7XG4gIH1cblxuICBhbmltYXRpb25TdGFydCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5yZWFsT3BlblN0YXRlKSB7XG4gICAgICB0aGlzLmFuaW1hdGlvbk9wZW5TdGF0ZSA9IHRydWU7XG4gICAgfVxuICB9XG5cbiAgYW5pbWF0aW9uRG9uZSgpOiB2b2lkIHtcbiAgICB0aGlzLmFuaW1hdGlvbk9wZW5TdGF0ZSA9IHRoaXMucmVhbE9wZW5TdGF0ZTtcbiAgfVxufVxuXG5leHBvcnQgdHlwZSBSYW5nZVBhcnRUeXBlID0gJ2xlZnQnIHwgJ3JpZ2h0JztcbiJdfQ==