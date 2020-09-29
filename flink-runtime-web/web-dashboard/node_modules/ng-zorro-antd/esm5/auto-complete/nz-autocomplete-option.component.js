/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, EventEmitter, Input, Output, ViewEncapsulation } from '@angular/core';
import { InputBoolean } from '../core/util/convert';
import { scrollIntoView } from '../core/util/scroll-into-view-if-needed';
var NzOptionSelectionChange = /** @class */ (function () {
    function NzOptionSelectionChange(source, isUserInput) {
        if (isUserInput === void 0) { isUserInput = false; }
        this.source = source;
        this.isUserInput = isUserInput;
    }
    return NzOptionSelectionChange;
}());
export { NzOptionSelectionChange };
if (false) {
    /** @type {?} */
    NzOptionSelectionChange.prototype.source;
    /** @type {?} */
    NzOptionSelectionChange.prototype.isUserInput;
}
var NzAutocompleteOptionComponent = /** @class */ (function () {
    function NzAutocompleteOptionComponent(changeDetectorRef, element) {
        this.changeDetectorRef = changeDetectorRef;
        this.element = element;
        this.nzDisabled = false;
        this.selectionChange = new EventEmitter();
        this.active = false;
        this.selected = false;
    }
    /**
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.select = /**
     * @return {?}
     */
    function () {
        this.selected = true;
        this.changeDetectorRef.markForCheck();
        this.emitSelectionChangeEvent();
    };
    /**
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.deselect = /**
     * @return {?}
     */
    function () {
        this.selected = false;
        this.changeDetectorRef.markForCheck();
        this.emitSelectionChangeEvent();
    };
    /** Git display label */
    /**
     * Git display label
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.getLabel = /**
     * Git display label
     * @return {?}
     */
    function () {
        return this.nzLabel || this.nzValue.toString();
    };
    /** Set active (only styles) */
    /**
     * Set active (only styles)
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.setActiveStyles = /**
     * Set active (only styles)
     * @return {?}
     */
    function () {
        if (!this.active) {
            this.active = true;
            this.changeDetectorRef.markForCheck();
        }
    };
    /** Unset active (only styles) */
    /**
     * Unset active (only styles)
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.setInactiveStyles = /**
     * Unset active (only styles)
     * @return {?}
     */
    function () {
        if (this.active) {
            this.active = false;
            this.changeDetectorRef.markForCheck();
        }
    };
    /**
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.scrollIntoViewIfNeeded = /**
     * @return {?}
     */
    function () {
        scrollIntoView(this.element.nativeElement);
    };
    /**
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.selectViaInteraction = /**
     * @return {?}
     */
    function () {
        if (!this.nzDisabled) {
            this.selected = !this.selected;
            if (this.selected) {
                this.setActiveStyles();
            }
            else {
                this.setInactiveStyles();
            }
            this.emitSelectionChangeEvent(true);
            this.changeDetectorRef.markForCheck();
        }
    };
    /**
     * @private
     * @param {?=} isUserInput
     * @return {?}
     */
    NzAutocompleteOptionComponent.prototype.emitSelectionChangeEvent = /**
     * @private
     * @param {?=} isUserInput
     * @return {?}
     */
    function (isUserInput) {
        if (isUserInput === void 0) { isUserInput = false; }
        this.selectionChange.emit(new NzOptionSelectionChange(this, isUserInput));
    };
    NzAutocompleteOptionComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-auto-option',
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    template: "<ng-content></ng-content>",
                    host: {
                        role: 'menuitem',
                        class: 'ant-select-dropdown-menu-item',
                        '[class.ant-select-dropdown-menu-item-selected]': 'selected',
                        '[class.ant-select-dropdown-menu-item-active]': 'active',
                        '[class.ant-select-dropdown-menu-item-disabled]': 'nzDisabled',
                        '[attr.aria-selected]': 'selected.toString()',
                        '[attr.aria-disabled]': 'nzDisabled.toString()',
                        '(click)': 'selectViaInteraction()',
                        '(mousedown)': '$event.preventDefault()'
                    }
                }] }
    ];
    /** @nocollapse */
    NzAutocompleteOptionComponent.ctorParameters = function () { return [
        { type: ChangeDetectorRef },
        { type: ElementRef }
    ]; };
    NzAutocompleteOptionComponent.propDecorators = {
        nzValue: [{ type: Input }],
        nzLabel: [{ type: Input }],
        nzDisabled: [{ type: Input }],
        selectionChange: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Object)
    ], NzAutocompleteOptionComponent.prototype, "nzDisabled", void 0);
    return NzAutocompleteOptionComponent;
}());
export { NzAutocompleteOptionComponent };
if (false) {
    /** @type {?} */
    NzAutocompleteOptionComponent.prototype.nzValue;
    /** @type {?} */
    NzAutocompleteOptionComponent.prototype.nzLabel;
    /** @type {?} */
    NzAutocompleteOptionComponent.prototype.nzDisabled;
    /** @type {?} */
    NzAutocompleteOptionComponent.prototype.selectionChange;
    /** @type {?} */
    NzAutocompleteOptionComponent.prototype.active;
    /** @type {?} */
    NzAutocompleteOptionComponent.prototype.selected;
    /**
     * @type {?}
     * @private
     */
    NzAutocompleteOptionComponent.prototype.changeDetectorRef;
    /**
     * @type {?}
     * @private
     */
    NzAutocompleteOptionComponent.prototype.element;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYXV0b2NvbXBsZXRlLW9wdGlvbi5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiYXV0by1jb21wbGV0ZS9uei1hdXRvY29tcGxldGUtb3B0aW9uLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxVQUFVLEVBQ1YsWUFBWSxFQUNaLEtBQUssRUFDTCxNQUFNLEVBQ04saUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsY0FBYyxFQUFFLE1BQU0seUNBQXlDLENBQUM7QUFFekU7SUFDRSxpQ0FBbUIsTUFBcUMsRUFBUyxXQUE0QjtRQUE1Qiw0QkFBQSxFQUFBLG1CQUE0QjtRQUExRSxXQUFNLEdBQU4sTUFBTSxDQUErQjtRQUFTLGdCQUFXLEdBQVgsV0FBVyxDQUFpQjtJQUFHLENBQUM7SUFDbkcsOEJBQUM7QUFBRCxDQUFDLEFBRkQsSUFFQzs7OztJQURhLHlDQUE0Qzs7SUFBRSw4Q0FBbUM7O0FBRy9GO0lBNEJFLHVDQUFvQixpQkFBb0MsRUFBVSxPQUFtQjtRQUFqRSxzQkFBaUIsR0FBakIsaUJBQWlCLENBQW1CO1FBQVUsWUFBTyxHQUFQLE9BQU8sQ0FBWTtRQU41RCxlQUFVLEdBQUcsS0FBSyxDQUFDO1FBQ3pCLG9CQUFlLEdBQUcsSUFBSSxZQUFZLEVBQTJCLENBQUM7UUFFakYsV0FBTSxHQUFHLEtBQUssQ0FBQztRQUNmLGFBQVEsR0FBRyxLQUFLLENBQUM7SUFFdUUsQ0FBQzs7OztJQUV6Riw4Q0FBTTs7O0lBQU47UUFDRSxJQUFJLENBQUMsUUFBUSxHQUFHLElBQUksQ0FBQztRQUNyQixJQUFJLENBQUMsaUJBQWlCLENBQUMsWUFBWSxFQUFFLENBQUM7UUFDdEMs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SUFDZixpREFBaUI7Ozs7O0lBRUwsMERBQTRDOzs7OztJQUFFLGdEQUEyQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBFdmVudEVtaXR0ZXIsXG4gIElucHV0LFxuICBPdXRwdXQsXG4gIFZpZXdFbmNhcHN1bGF0aW9uXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBJbnB1dEJvb2xlYW4gfSBmcm9tICcuLi9jb3JlL3V0aWwvY29udmVydCc7XG5pbXBvcnQgeyBzY3JvbGxJbnRvVmlldyB9IGZyb20gJy4uL2NvcmUvdXRpbC9zY3JvbGwtaW50by12aWV3LWlmLW5lZWRlZCc7XG5cbmV4cG9ydCBjbGFzcyBOek9wdGlvblNlbGVjdGlvbkNoYW5nZSB7XG4gIGNvbnN0cnVjdG9yKHB1YmxpYyBzb3VyY2U6IE56QXV0b2NvbXBsZXRlT3B0aW9uQ29tcG9uZW50LCBwdWJsaWMgaXNVc2VySW5wdXQ6IGJvb2xlYW4gPSBmYWxzZSkge31cbn1cblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotYXV0by1vcHRpb24nLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1hdXRvY29tcGxldGUtb3B0aW9uLmNvbXBvbmVudC5odG1sJyxcbiAgaG9zdDoge1xuICAgIHJvbGU6ICdtZW51aXRlbScsXG4gICAgY2xhc3M6ICdhbnQtc2VsZWN0LWRyb3Bkb3duLW1lbnUtaXRlbScsXG4gICAgJ1tjbGFzcy5hbnQtc2VsZWN0LWRyb3Bkb3duLW1lbnUtaXRlbS1zZWxlY3RlZF0nOiAnc2VsZWN0ZWQnLFxuICAgICdbY2xhc3MuYW50LXNlbGVjdC1kcm9wZG93bi1tZW51LWl0ZW0tYWN0aXZlXSc6ICdhY3RpdmUnLFxuICAgICdbY2xhc3MuYW50LXNlbGVjdC1kcm9wZG93bi1tZW51LWl0ZW0tZGlzYWJsZWRdJzogJ256RGlzYWJsZWQnLFxuICAgICdbYXR0ci5hcmlhLXNlbGVjdGVkXSc6ICdzZWxlY3RlZC50b1N0cmluZygpJyxcbiAgICAnW2F0dHIuYXJpYS1kaXNhYmxlZF0nOiAnbnpEaXNhYmxlZC50b1N0cmluZygpJyxcbiAgICAnKGNsaWNrKSc6ICdzZWxlY3RWaWFJbnRlcmFjdGlvbigpJyxcbiAgICAnKG1vdXNlZG93biknOiAnJGV2ZW50LnByZXZlbnREZWZhdWx0KCknXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpBdXRvY29tcGxldGVPcHRpb25Db21wb25lbnQge1xuICAvKiB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55ICovXG4gIEBJbnB1dCgpIG56VmFsdWU6IGFueTtcbiAgQElucHV0KCkgbnpMYWJlbDogc3RyaW5nO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpEaXNhYmxlZCA9IGZhbHNlO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgc2VsZWN0aW9uQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxOek9wdGlvblNlbGVjdGlvbkNoYW5nZT4oKTtcblxuICBhY3RpdmUgPSBmYWxzZTtcbiAgc2VsZWN0ZWQgPSBmYWxzZTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIGNoYW5nZURldGVjdG9yUmVmOiBDaGFuZ2VEZXRlY3RvclJlZiwgcHJpdmF0ZSBlbGVtZW50OiBFbGVtZW50UmVmKSB7fVxuXG4gIHNlbGVjdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNlbGVjdGVkID0gdHJ1ZTtcbiAgICB0aGlzLmNoYW5nZURldGVjdG9yUmVmLm1hcmtGb3JDaGVjaygpO1xuICAgIHRoaXMuZW1pdFNlbGVjdGlvbkNoYW5nZUV2ZW50KCk7XG4gIH1cblxuICBkZXNlbGVjdCgpOiB2b2lkIHtcbiAgICB0aGlzLnNlbGVjdGVkID0gZmFsc2U7XG4gICAgdGhpcy5jaGFuZ2VEZXRlY3RvclJlZi5tYXJrRm9yQ2hlY2soKTtcbiAgICB0aGlzLmVtaXRTZWxlY3Rpb25DaGFuZ2VFdmVudCgpO1xuICB9XG5cbiAgLyoqIEdpdCBkaXNwbGF5IGxhYmVsICovXG4gIGdldExhYmVsKCk6IHN0cmluZyB7XG4gICAgcmV0dXJuIHRoaXMubnpMYWJlbCB8fCB0aGlzLm56VmFsdWUudG9TdHJpbmcoKTtcbiAgfVxuXG4gIC8qKiBTZXQgYWN0aXZlIChvbmx5IHN0eWxlcykgKi9cbiAgc2V0QWN0aXZlU3R5bGVzKCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5hY3RpdmUpIHtcbiAgICAgIHRoaXMuYWN0aXZlID0gdHJ1ZTtcbiAgICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYubWFya0ZvckNoZWNrKCk7XG4gICAgfVxuICB9XG5cbiAgLyoqIFVuc2V0IGFjdGl2ZSAob25seSBzdHlsZXMpICovXG4gIHNldEluYWN0aXZlU3R5bGVzKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLmFjdGl2ZSkge1xuICAgICAgdGhpcy5hY3RpdmUgPSBmYWxzZTtcbiAgICAgIHRoaXMuY2hhbmdlRGV0ZWN0b3JSZWYubWFya0ZvckNoZWNrKCk7XG4gICAgfVxuICB9XG5cbiAgc2Nyb2xsSW50b1ZpZXdJZk5lZWRlZCgpOiB2b2lkIHtcbiAgICBzY3JvbGxJbnRvVmlldyh0aGlzLmVsZW1lbnQubmF0aXZlRWxlbWVudCk7XG4gIH1cblxuICBzZWxlY3RWaWFJbnRlcmFjdGlvbigpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMubnpEaXNhYmxlZCkge1xuICAgICAgdGhpcy5zZWxlY3RlZCA9ICF0aGlzLnNlbGVjdGVkO1xuICAgICAgaWYgKHRoaXMuc2VsZWN0ZWQpIHtcbiAgICAgICAgdGhpcy5zZXRBY3RpdmVTdHlsZXMoKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuc2V0SW5hY3RpdmVTdHlsZXMoKTtcbiAgICAgIH1cbiAgICAgIHRoaXMuZW1pdFNlbGVjdGlvbkNoYW5nZUV2ZW50KHRydWUpO1xuICAgICAgdGhpcy5jaGFuZ2VEZXRlY3RvclJlZi5tYXJrRm9yQ2hlY2soKTtcbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGVtaXRTZWxlY3Rpb25DaGFuZ2VFdmVudChpc1VzZXJJbnB1dDogYm9vbGVhbiA9IGZhbHNlKTogdm9pZCB7XG4gICAgdGhpcy5zZWxlY3Rpb25DaGFuZ2UuZW1pdChuZXcgTnpPcHRpb25TZWxlY3Rpb25DaGFuZ2UodGhpcywgaXNVc2VySW5wdXQpKTtcbiAgfVxufVxuIl19