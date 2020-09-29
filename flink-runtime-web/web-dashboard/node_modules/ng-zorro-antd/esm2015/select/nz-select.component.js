/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { FocusMonitor } from '@angular/cdk/a11y';
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { forwardRef, ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChildren, ElementRef, EventEmitter, Host, Input, Optional, Output, QueryList, Renderer2, TemplateRef, ViewChild, ViewEncapsulation } from '@angular/core';
import { NG_VALUE_ACCESSOR } from '@angular/forms';
import { merge, EMPTY, Subject } from 'rxjs';
import { flatMap, startWith, takeUntil } from 'rxjs/operators';
import { slideMotion } from '../core/animation/slide';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { isNotNil } from '../core/util/check';
import { toBoolean, InputBoolean } from '../core/util/convert';
import { NzOptionGroupComponent } from './nz-option-group.component';
import { NzOptionComponent } from './nz-option.component';
import { NzSelectTopControlComponent } from './nz-select-top-control.component';
import { NzSelectService } from './nz-select.service';
export class NzSelectComponent {
    /**
     * @param {?} renderer
     * @param {?} nzSelectService
     * @param {?} cdr
     * @param {?} focusMonitor
     * @param {?} elementRef
     * @param {?=} noAnimation
     */
    constructor(renderer, nzSelectService, cdr, focusMonitor, elementRef, noAnimation) {
        this.renderer = renderer;
        this.nzSelectService = nzSelectService;
        this.cdr = cdr;
        this.focusMonitor = focusMonitor;
        this.noAnimation = noAnimation;
        this.open = false;
        this.onChange = (/**
         * @return {?}
         */
        () => null);
        this.onTouched = (/**
         * @return {?}
         */
        () => null);
        this.dropDownPosition = 'bottom';
        this._disabled = false;
        this._autoFocus = false;
        this.isInit = false;
        this.destroy$ = new Subject();
        this.nzOnSearch = new EventEmitter();
        this.nzScrollToBottom = new EventEmitter();
        this.nzOpenChange = new EventEmitter();
        this.nzBlur = new EventEmitter();
        this.nzFocus = new EventEmitter();
        this.nzSize = 'default';
        this.nzDropdownMatchSelectWidth = true;
        this.nzAllowClear = false;
        this.nzShowSearch = false;
        this.nzLoading = false;
        this.nzShowArrow = true;
        this.nzTokenSeparators = [];
        renderer.addClass(elementRef.nativeElement, 'ant-select');
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzAutoClearSearchValue(value) {
        this.nzSelectService.autoClearSearchValue = toBoolean(value);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzMaxMultipleCount(value) {
        this.nzSelectService.maxMultipleCount = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzServerSearch(value) {
        this.nzSelectService.serverSearch = toBoolean(value);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzMode(value) {
        this.nzSelectService.mode = value;
        this.nzSelectService.check();
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzFilterOption(value) {
        this.nzSelectService.filterOption = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set compareWith(value) {
        this.nzSelectService.compareWith = value;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzAutoFocus(value) {
        this._autoFocus = toBoolean(value);
        this.updateAutoFocus();
    }
    /**
     * @return {?}
     */
    get nzAutoFocus() {
        return this._autoFocus;
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzOpen(value) {
        this.open = value;
        this.nzSelectService.setOpenState(value);
    }
    /**
     * @param {?} value
     * @return {?}
     */
    set nzDisabled(value) {
        this._disabled = toBoolean(value);
        this.nzSelectService.disabled = this._disabled;
        this.nzSelectService.check();
        if (this.nzDisabled && this.isInit) {
            this.closeDropDown();
        }
    }
    /**
     * @return {?}
     */
    get nzDisabled() {
        return this._disabled;
    }
    /**
     * @return {?}
     */
    updateAutoFocus() {
        if (this.nzSelectTopControlComponent.inputElement) {
            if (this.nzAutoFocus) {
                this.renderer.setAttribute(this.nzSelectTopControlComponent.inputElement.nativeElement, 'autofocus', 'autofocus');
            }
            else {
                this.renderer.removeAttribute(this.nzSelectTopControlComponent.inputElement.nativeElement, 'autofocus');
            }
        }
    }
    /**
     * @return {?}
     */
    focus() {
        if (this.nzSelectTopControlComponent.inputElement) {
            this.focusMonitor.focusVia(this.nzSelectTopControlComponent.inputElement, 'keyboard');
            this.nzFocus.emit();
        }
    }
    /**
     * @return {?}
     */
    blur() {
        if (this.nzSelectTopControlComponent.inputElement) {
            this.nzSelectTopControlComponent.inputElement.nativeElement.blur();
            this.nzBlur.emit();
        }
    }
    /**
     * @param {?} event
     * @return {?}
     */
    onKeyDown(event) {
        this.nzSelectService.onKeyDown(event);
    }
    /**
     * @return {?}
     */
    toggleDropDown() {
        if (!this.nzDisabled) {
            this.nzSelectService.setOpenState(!this.open);
        }
    }
    /**
     * @return {?}
     */
    closeDropDown() {
        this.nzSelectService.setOpenState(false);
    }
    /**
     * @param {?} position
     * @return {?}
     */
    onPositionChange(position) {
        this.dropDownPosition = position.connectionPair.originY;
    }
    /**
     * @return {?}
     */
    updateCdkConnectedOverlayStatus() {
        this.triggerWidth = this.cdkOverlayOrigin.elementRef.nativeElement.getBoundingClientRect().width;
    }
    /**
     * @return {?}
     */
    updateCdkConnectedOverlayPositions() {
        setTimeout((/**
         * @return {?}
         */
        () => {
            if (this.cdkConnectedOverlay && this.cdkConnectedOverlay.overlayRef) {
                this.cdkConnectedOverlay.overlayRef.updatePosition();
            }
        }));
    }
    /**
     * update ngModel -> update listOfSelectedValue *
     * @param {?} value
     * @return {?}
     */
    // tslint:disable-next-line:no-any
    writeValue(value) {
        this.value = value;
        /** @type {?} */
        let listValue = [];
        if (isNotNil(value)) {
            if (Array.isArray(value)) {
                listValue = value;
            }
            else {
                listValue = [value];
            }
        }
        this.nzSelectService.updateListOfSelectedValue(listValue, false);
        this.cdr.markForCheck();
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnChange(fn) {
        this.onChange = fn;
    }
    /**
     * @param {?} fn
     * @return {?}
     */
    registerOnTouched(fn) {
        this.onTouched = fn;
    }
    /**
     * @param {?} isDisabled
     * @return {?}
     */
    setDisabledState(isDisabled) {
        this.nzDisabled = isDisabled;
        this.cdr.markForCheck();
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.nzSelectService.searchValue$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} data
         * @return {?}
         */
        data => {
            this.nzOnSearch.emit(data);
            this.updateCdkConnectedOverlayPositions();
        }));
        this.nzSelectService.modelChange$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} modelValue
         * @return {?}
         */
        modelValue => {
            if (this.value !== modelValue) {
                this.value = modelValue;
                this.onChange(this.value);
                this.updateCdkConnectedOverlayPositions();
            }
        }));
        this.nzSelectService.open$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} value
         * @return {?}
         */
        value => {
            if (this.open !== value) {
                this.nzOpenChange.emit(value);
            }
            if (value) {
                this.focus();
                this.updateCdkConnectedOverlayStatus();
            }
            else {
                this.blur();
                this.onTouched();
            }
            this.open = value;
        }));
        this.nzSelectService.check$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @return {?}
         */
        () => {
            this.cdr.markForCheck();
        }));
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        this.updateCdkConnectedOverlayStatus();
        this.isInit = true;
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.listOfNzOptionGroupComponent.changes
            .pipe(startWith(true), flatMap((/**
         * @return {?}
         */
        () => merge(this.listOfNzOptionGroupComponent.changes, this.listOfNzOptionComponent.changes, ...this.listOfNzOptionGroupComponent.map((/**
         * @param {?} group
         * @return {?}
         */
        group => group.listOfNzOptionComponent ? group.listOfNzOptionComponent.changes : EMPTY))).pipe(startWith(true)))))
            .subscribe((/**
         * @return {?}
         */
        () => {
            this.nzSelectService.updateTemplateOption(this.listOfNzOptionComponent.toArray(), this.listOfNzOptionGroupComponent.toArray());
        }));
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
}
NzSelectComponent.decorators = [
    { type: Component, args: [{
                selector: 'nz-select',
                preserveWhitespaces: false,
                providers: [
                    NzSelectService,
                    {
                        provide: NG_VALUE_ACCESSOR,
                        useExisting: forwardRef((/**
                         * @return {?}
                         */
                        () => NzSelectComponent)),
                        multi: true
                    }
                ],
                changeDetection: ChangeDetectionStrategy.OnPush,
                encapsulation: ViewEncapsulation.None,
                animations: [slideMotion],
                template: "<div cdkOverlayOrigin\n  nz-select-top-control\n  tabindex=\"0\"\n  class=\"ant-select-selection\"\n  [nzOpen]=\"open\"\n  [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n  [nzMaxTagPlaceholder]=\"nzMaxTagPlaceholder\"\n  [nzPlaceHolder]=\"nzPlaceHolder\"\n  [nzAllowClear]=\"nzAllowClear\"\n  [nzMaxTagCount]=\"nzMaxTagCount\"\n  [nzShowArrow]=\"nzShowArrow\"\n  [nzLoading]=\"nzLoading\"\n  [nzSuffixIcon]=\"nzSuffixIcon\"\n  [nzClearIcon]=\"nzClearIcon\"\n  [nzRemoveIcon]=\"nzRemoveIcon\"\n  [nzShowSearch]=\"nzShowSearch\"\n  [nzTokenSeparators]=\"nzTokenSeparators\"\n  [class.ant-select-selection--single]=\"nzSelectService.isSingleMode\"\n  [class.ant-select-selection--multiple]=\"nzSelectService.isMultipleOrTags\"\n  (keydown)=\"onKeyDown($event)\">\n</div>\n<ng-template\n  cdkConnectedOverlay\n  nzConnectedOverlay\n  [cdkConnectedOverlayHasBackdrop]=\"true\"\n  [cdkConnectedOverlayMinWidth]=\"nzDropdownMatchSelectWidth? null : triggerWidth\"\n  [cdkConnectedOverlayWidth]=\"nzDropdownMatchSelectWidth? triggerWidth : null\"\n  [cdkConnectedOverlayOrigin]=\"cdkOverlayOrigin\"\n  (backdropClick)=\"closeDropDown()\"\n  (detach)=\"closeDropDown();\"\n  (positionChange)=\"onPositionChange($event)\"\n  [cdkConnectedOverlayOpen]=\"open\">\n  <div\n    class=\"ant-select-dropdown\"\n    [class.ant-select-dropdown--single]=\"nzSelectService.isSingleMode\"\n    [class.ant-select-dropdown--multiple]=\"nzSelectService.isMultipleOrTags\"\n    [class.ant-select-dropdown-placement-bottomLeft]=\"dropDownPosition === 'bottom'\"\n    [class.ant-select-dropdown-placement-topLeft]=\"dropDownPosition === 'top'\"\n    [nzClassListAdd]=\"[nzDropdownClassName]\"\n    [@slideMotion]=\"dropDownPosition\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [ngStyle]=\"nzDropdownStyle\">\n    <div nz-option-container\n      style=\"overflow: auto;transform: translateZ(0px);\"\n      (keydown)=\"onKeyDown($event)\"\n      [nzMenuItemSelectedIcon]=\"nzMenuItemSelectedIcon\"\n      [nzNotFoundContent]=\"nzNotFoundContent\"\n      (nzScrollToBottom)=\"nzScrollToBottom.emit()\">\n    </div>\n    <ng-template [ngTemplateOutlet]=\"nzDropdownRender\"></ng-template>\n  </div>\n</ng-template>\n<!--can not use ViewChild since it will match sub options in option group -->\n<ng-template>\n  <ng-content></ng-content>\n</ng-template>",
                host: {
                    '[class.ant-select-lg]': 'nzSize==="large"',
                    '[class.ant-select-sm]': 'nzSize==="small"',
                    '[class.ant-select-enabled]': '!nzDisabled',
                    '[class.ant-select-no-arrow]': '!nzShowArrow',
                    '[class.ant-select-disabled]': 'nzDisabled',
                    '[class.ant-select-allow-clear]': 'nzAllowClear',
                    '[class.ant-select-open]': 'open',
                    '(click)': 'toggleDropDown()'
                },
                styles: [`
      .ant-select-dropdown {
        top: 100%;
        left: 0;
        position: relative;
        width: 100%;
        margin-top: 4px;
        margin-bottom: 4px;
      }
    `]
            }] }
];
/** @nocollapse */
NzSelectComponent.ctorParameters = () => [
    { type: Renderer2 },
    { type: NzSelectService },
    { type: ChangeDetectorRef },
    { type: FocusMonitor },
    { type: ElementRef },
    { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
];
NzSelectComponent.propDecorators = {
    cdkOverlayOrigin: [{ type: ViewChild, args: [CdkOverlayOrigin,] }],
    cdkConnectedOverlay: [{ type: ViewChild, args: [CdkConnectedOverlay,] }],
    nzSelectTopControlComponent: [{ type: ViewChild, args: [NzSelectTopControlComponent,] }],
    listOfNzOptionComponent: [{ type: ContentChildren, args: [NzOptionComponent,] }],
    listOfNzOptionGroupComponent: [{ type: ContentChildren, args: [NzOptionGroupComponent,] }],
    nzOnSearch: [{ type: Output }],
    nzScrollToBottom: [{ type: Output }],
    nzOpenChange: [{ type: Output }],
    nzBlur: [{ type: Output }],
    nzFocus: [{ type: Output }],
    nzSize: [{ type: Input }],
    nzDropdownClassName: [{ type: Input }],
    nzDropdownMatchSelectWidth: [{ type: Input }],
    nzDropdownStyle: [{ type: Input }],
    nzNotFoundContent: [{ type: Input }],
    nzAllowClear: [{ type: Input }],
    nzShowSearch: [{ type: Input }],
    nzLoading: [{ type: Input }],
    nzPlaceHolder: [{ type: Input }],
    nzMaxTagCount: [{ type: Input }],
    nzDropdownRender: [{ type: Input }],
    nzSuffixIcon: [{ type: Input }],
    nzClearIcon: [{ type: Input }],
    nzRemoveIcon: [{ type: Input }],
    nzMenuItemSelectedIcon: [{ type: Input }],
    nzShowArrow: [{ type: Input }],
    nzTokenSeparators: [{ type: Input }],
    nzMaxTagPlaceholder: [{ type: Input }],
    nzAutoClearSearchValue: [{ type: Input }],
    nzMaxMultipleCount: [{ type: Input }],
    nzServerSearch: [{ type: Input }],
    nzMode: [{ type: Input }],
    nzFilterOption: [{ type: Input }],
    compareWith: [{ type: Input }],
    nzAutoFocus: [{ type: Input }],
    nzOpen: [{ type: Input }],
    nzDisabled: [{ type: Input }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSelectComponent.prototype, "nzAllowClear", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSelectComponent.prototype, "nzShowSearch", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSelectComponent.prototype, "nzLoading", void 0);
if (false) {
    /** @type {?} */
    NzSelectComponent.prototype.open;
    /** @type {?} */
    NzSelectComponent.prototype.value;
    /** @type {?} */
    NzSelectComponent.prototype.onChange;
    /** @type {?} */
    NzSelectComponent.prototype.onTouched;
    /** @type {?} */
    NzSelectComponent.prototype.dropDownPosition;
    /** @type {?} */
    NzSelectComponent.prototype.triggerWidth;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype._disabled;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype._autoFocus;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype.isInit;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype.destroy$;
    /** @type {?} */
    NzSelectComponent.prototype.cdkOverlayOrigin;
    /** @type {?} */
    NzSelectComponent.prototype.cdkConnectedOverlay;
    /** @type {?} */
    NzSelectComponent.prototype.nzSelectTopControlComponent;
    /**
     * should move to nz-option-container when https://github.com/angular/angular/issues/20810 resolved *
     * @type {?}
     */
    NzSelectComponent.prototype.listOfNzOptionComponent;
    /** @type {?} */
    NzSelectComponent.prototype.listOfNzOptionGroupComponent;
    /** @type {?} */
    NzSelectComponent.prototype.nzOnSearch;
    /** @type {?} */
    NzSelectComponent.prototype.nzScrollToBottom;
    /** @type {?} */
    NzSelectComponent.prototype.nzOpenChange;
    /** @type {?} */
    NzSelectComponent.prototype.nzBlur;
    /** @type {?} */
    NzSelectComponent.prototype.nzFocus;
    /** @type {?} */
    NzSelectComponent.prototype.nzSize;
    /** @type {?} */
    NzSelectComponent.prototype.nzDropdownClassName;
    /** @type {?} */
    NzSelectComponent.prototype.nzDropdownMatchSelectWidth;
    /** @type {?} */
    NzSelectComponent.prototype.nzDropdownStyle;
    /** @type {?} */
    NzSelectComponent.prototype.nzNotFoundContent;
    /** @type {?} */
    NzSelectComponent.prototype.nzAllowClear;
    /** @type {?} */
    NzSelectComponent.prototype.nzShowSearch;
    /** @type {?} */
    NzSelectComponent.prototype.nzLoading;
    /** @type {?} */
    NzSelectComponent.prototype.nzPlaceHolder;
    /** @type {?} */
    NzSelectComponent.prototype.nzMaxTagCount;
    /** @type {?} */
    NzSelectComponent.prototype.nzDropdownRender;
    /** @type {?} */
    NzSelectComponent.prototype.nzSuffixIcon;
    /** @type {?} */
    NzSelectComponent.prototype.nzClearIcon;
    /** @type {?} */
    NzSelectComponent.prototype.nzRemoveIcon;
    /** @type {?} */
    NzSelectComponent.prototype.nzMenuItemSelectedIcon;
    /** @type {?} */
    NzSelectComponent.prototype.nzShowArrow;
    /** @type {?} */
    NzSelectComponent.prototype.nzTokenSeparators;
    /** @type {?} */
    NzSelectComponent.prototype.nzMaxTagPlaceholder;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype.renderer;
    /** @type {?} */
    NzSelectComponent.prototype.nzSelectService;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype.cdr;
    /**
     * @type {?}
     * @private
     */
    NzSelectComponent.prototype.focusMonitor;
    /** @type {?} */
    NzSelectComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc2VsZWN0LmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJzZWxlY3Qvbnotc2VsZWN0LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxtQkFBbUIsQ0FBQztBQUNqRCxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsZ0JBQWdCLEVBQWtDLE1BQU0sc0JBQXNCLENBQUM7QUFDN0csT0FBTyxFQUNMLFVBQVUsRUFHVix1QkFBdUIsRUFDdkIsaUJBQWlCLEVBQ2pCLFNBQVMsRUFDVCxlQUFlLEVBQ2YsVUFBVSxFQUNWLFlBQVksRUFDWixJQUFJLEVBQ0osS0FBSyxFQUdMLFFBQVEsRUFDUixNQUFNLEVBQ04sU0FBUyxFQUNULFNBQVMsRUFDVCxXQUFXLEVBQ1gsU0FBUyxFQUNULGlCQUFpQixFQUNsQixNQUFNLGVBQWUsQ0FBQztBQUN2QixPQUFPLEVBQXdCLGlCQUFpQixFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFDekUsT0FBTyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQUUsT0FBTyxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQzdDLE9BQU8sRUFBRSxPQUFPLEVBQUUsU0FBUyxFQUFFLFNBQVMsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBQy9ELE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUN0RCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSxnREFBZ0QsQ0FBQztBQUV4RixPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFDOUMsT0FBTyxFQUFFLFNBQVMsRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUMvRCxPQUFPLEVBQUUsc0JBQXNCLEVBQUUsTUFBTSw2QkFBNkIsQ0FBQztBQUNyRSxPQUFPLEVBQUUsaUJBQWlCLEVBQUUsTUFBTSx1QkFBdUIsQ0FBQztBQUUx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IGZyb20gJ0Bhbmd1bGFyL2Nkay9hMTF5JztcbmltcG9ydCB7IENka0Nvbm5lY3RlZE92ZXJsYXksIENka092ZXJsYXlPcmlnaW4sIENvbm5lY3RlZE92ZXJsYXlQb3NpdGlvbkNoYW5nZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7XG4gIGZvcndhcmRSZWYsXG4gIEFmdGVyQ29udGVudEluaXQsXG4gIEFmdGVyVmlld0luaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGRyZW4sXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSG9zdCxcbiAgSW5wdXQsXG4gIE9uRGVzdHJveSxcbiAgT25Jbml0LFxuICBPcHRpb25hbCxcbiAgT3V0cHV0LFxuICBRdWVyeUxpc3QsXG4gIFJlbmRlcmVyMixcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBDb250cm9sVmFsdWVBY2Nlc3NvciwgTkdfVkFMVUVfQUNDRVNTT1IgfSBmcm9tICdAYW5ndWxhci9mb3Jtcyc7XG5pbXBvcnQgeyBtZXJnZSwgRU1QVFksIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGZsYXRNYXAsIHN0YXJ0V2l0aCwgdGFrZVVudGlsIH0gZnJvbSAncnhqcy9vcGVyYXRvcnMnO1xuaW1wb3J0IHsgc2xpZGVNb3Rpb24gfSBmcm9tICcuLi9jb3JlL2FuaW1hdGlvbi9zbGlkZSc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uRGlyZWN0aXZlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOelNpemVMRFNUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9zaXplJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcbmltcG9ydCB7IHRvQm9vbGVhbiwgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpPcHRpb25Hcm91cENvbXBvbmVudCB9IGZyb20gJy4vbnotb3B0aW9uLWdyb3VwLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOek9wdGlvbkNvbXBvbmVudCB9IGZyb20gJy4vbnotb3B0aW9uLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBURmlsdGVyT3B0aW9uIH0gZnJvbSAnLi9uei1vcHRpb24ucGlwZSc7XG5pbXBvcnQgeyBOelNlbGVjdFRvcENvbnRyb2xDb21wb25lbnQgfSBmcm9tICcuL256LXNlbGVjdC10b3AtY29udHJvbC5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpTZWxlY3RTZXJ2aWNlIH0gZnJvbSAnLi9uei1zZWxlY3Quc2VydmljZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ256LXNlbGVjdCcsXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBwcm92aWRlcnM6IFtcbiAgICBOelNlbGVjdFNlcnZpY2UsXG4gICAge1xuICAgICAgcHJvdmlkZTogTkdfVkFMVUVfQUNDRVNTT1IsXG4gICAgICB1c2VFeGlzdGluZzogZm9yd2FyZFJlZigoKSA9PiBOelNlbGVjdENvbXBvbmVudCksXG4gICAgICBtdWx0aTogdHJ1ZVxuICAgIH1cbiAgXSxcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIGFuaW1hdGlvbnM6IFtzbGlkZU1vdGlvbl0sXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1zZWxlY3QuY29tcG9uZW50Lmh0bWwnLFxuICBob3N0OiB7XG4gICAgJ1tjbGFzcy5hbnQtc2VsZWN0LWxnXSc6ICduelNpemU9PT1cImxhcmdlXCInLFxuICAgICdbY2xhc3MuYW50LXNlbGVjdC1zbV0nOiAnbnpTaXplPT09XCJzbWFsbFwiJyxcbiAgICAnW2NsYXNzLmFudC1zZWxlY3QtZW5hYmxlZF0nOiAnIW56RGlzYWJsZWQnLFxuICAgICdbY2xhc3MuYW50LXNlbGVjdC1uby1hcnJvd10nOiAnIW56U2hvd0Fycm93JyxcbiAgICAnW2NsYXNzLmFudC1zZWxlY3QtZGlzYWJsZWRdJzogJ256RGlzYWJsZWQnLFxuICAgICdbY2xhc3MuYW50LXNlbGVjdC1hbGxvdy1jbGVhcl0nOiAnbnpBbGxvd0NsZWFyJyxcbiAgICAnW2NsYXNzLmFudC1zZWxlY3Qtb3Blbl0nOiAnb3BlbicsXG4gICAgJyhjbGljayknOiAndG9nZ2xlRHJvcERvd24oKSdcbiAgfSxcbiAgc3R5bGVzOiBbXG4gICAgYFxuICAgICAgLmFudC1zZWxlY3QtZHJvcGRvd24ge1xuICAgICAgICB0b3A6IDEwMCU7XG4gICAgICAgIGxlZnQ6IDA7XG4gICAgICAgIHBvc2l0aW9uOiByZWxhdGl2ZTtcbiAgICAgICAgd2lkdGg6IDEwMCU7XG4gICAgICAgIG1hcmdpbi10b3A6IDRweDtcbiAgICAgICAgbWFyZ2luLWJvdHRvbTogNHB4O1xuICAgICAgfVxuICAgIGBcbiAgXVxufSlcbmV4cG9ydCBjbGFzcyBOelNlbGVjdENvbXBvbmVudCBpbXBsZW1lbnRzIENvbnRyb2xWYWx1ZUFjY2Vzc29yLCBPbkluaXQsIEFmdGVyVmlld0luaXQsIE9uRGVzdHJveSwgQWZ0ZXJDb250ZW50SW5pdCB7XG4gIG9wZW4gPSBmYWxzZTtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICB2YWx1ZTogYW55IHwgYW55W107XG4gIG9uQ2hhbmdlOiAodmFsdWU6IHN0cmluZyB8IHN0cmluZ1tdKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgb25Ub3VjaGVkOiAoKSA9PiB2b2lkID0gKCkgPT4gbnVsbDtcbiAgZHJvcERvd25Qb3NpdGlvbjogJ3RvcCcgfCAnY2VudGVyJyB8ICdib3R0b20nID0gJ2JvdHRvbSc7XG4gIHRyaWdnZXJXaWR0aDogbnVtYmVyO1xuICBwcml2YXRlIF9kaXNhYmxlZCA9IGZhbHNlO1xuICBwcml2YXRlIF9hdXRvRm9jdXMgPSBmYWxzZTtcbiAgcHJpdmF0ZSBpc0luaXQgPSBmYWxzZTtcbiAgcHJpdmF0ZSBkZXN0cm95JCA9IG5ldyBTdWJqZWN0KCk7XG4gIEBWaWV3Q2hpbGQoQ2RrT3ZlcmxheU9yaWdpbikgY2RrT3ZlcmxheU9yaWdpbjogQ2RrT3ZlcmxheU9yaWdpbjtcbiAgQFZpZXdDaGlsZChDZGtDb25uZWN0ZWRPdmVybGF5KSBjZGtDb25uZWN0ZWRPdmVybGF5OiBDZGtDb25uZWN0ZWRPdmVybGF5O1xuICBAVmlld0NoaWxkKE56U2VsZWN0VG9wQ29udHJvbENvbXBvbmVudCkgbnpTZWxlY3RUb3BDb250cm9sQ29tcG9uZW50OiBOelNlbGVjdFRvcENvbnRyb2xDb21wb25lbnQ7XG4gIC8qKiBzaG91bGQgbW92ZSB0byBuei1vcHRpb24tY29udGFpbmVyIHdoZW4gaHR0cHM6Ly9naXRodWIuY29tL2FuZ3VsYXIvYW5ndWxhci9pc3N1ZXMvMjA4MTAgcmVzb2x2ZWQgKiovXG4gIEBDb250ZW50Q2hpbGRyZW4oTnpPcHRpb25Db21wb25lbnQpIGxpc3RPZk56T3B0aW9uQ29tcG9uZW50OiBRdWVyeUxpc3Q8TnpPcHRpb25Db21wb25lbnQ+O1xuICBAQ29udGVudENoaWxkcmVuKE56T3B0aW9uR3JvdXBDb21wb25lbnQpIGxpc3RPZk56T3B0aW9uR3JvdXBDb21wb25lbnQ6IFF1ZXJ5TGlzdDxOek9wdGlvbkdyb3VwQ29tcG9uZW50PjtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25TZWFyY2ggPSBuZXcgRXZlbnRFbWl0dGVyPHN0cmluZz4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56U2Nyb2xsVG9Cb3R0b20gPSBuZXcgRXZlbnRFbWl0dGVyPHZvaWQ+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9wZW5DaGFuZ2UgPSBuZXcgRXZlbnRFbWl0dGVyPGJvb2xlYW4+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuekJsdXIgPSBuZXcgRXZlbnRFbWl0dGVyPHZvaWQ+KCk7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuekZvY3VzID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuICBASW5wdXQoKSBuelNpemU6IE56U2l6ZUxEU1R5cGUgPSAnZGVmYXVsdCc7XG4gIEBJbnB1dCgpIG56RHJvcGRvd25DbGFzc05hbWU6IHN0cmluZztcbiAgQElucHV0KCkgbnpEcm9wZG93bk1hdGNoU2VsZWN0V2lkdGggPSB0cnVlO1xuICBASW5wdXQoKSBuekRyb3Bkb3duU3R5bGU6IHsgW2tleTogc3RyaW5nXTogc3RyaW5nIH07XG4gIEBJbnB1dCgpIG56Tm90Rm91bmRDb250ZW50OiBzdHJpbmc7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekFsbG93Q2xlYXIgPSBmYWxzZTtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56U2hvd1NlYXJjaCA9IGZhbHNlO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpMb2FkaW5nID0gZmFsc2U7XG4gIEBJbnB1dCgpIG56UGxhY2VIb2xkZXI6IHN0cmluZztcbiAgQElucHV0KCkgbnpNYXhUYWdDb3VudDogbnVtYmVyO1xuICBASW5wdXQoKSBuekRyb3Bkb3duUmVuZGVyOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpTdWZmaXhJY29uOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpDbGVhckljb246IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuelJlbW92ZUljb246IFRlbXBsYXRlUmVmPHZvaWQ+O1xuICBASW5wdXQoKSBuek1lbnVJdGVtU2VsZWN0ZWRJY29uOiBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpTaG93QXJyb3cgPSB0cnVlO1xuICBASW5wdXQoKSBuelRva2VuU2VwYXJhdG9yczogc3RyaW5nW10gPSBbXTtcbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBASW5wdXQoKSBuek1heFRhZ1BsYWNlaG9sZGVyOiBUZW1wbGF0ZVJlZjx7ICRpbXBsaWNpdDogYW55W10gfT47XG5cbiAgQElucHV0KClcbiAgc2V0IG56QXV0b0NsZWFyU2VhcmNoVmFsdWUodmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5hdXRvQ2xlYXJTZWFyY2hWYWx1ZSA9IHRvQm9vbGVhbih2YWx1ZSk7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpNYXhNdWx0aXBsZUNvdW50KHZhbHVlOiBudW1iZXIpIHtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5tYXhNdWx0aXBsZUNvdW50ID0gdmFsdWU7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpTZXJ2ZXJTZWFyY2godmFsdWU6IGJvb2xlYW4pIHtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5zZXJ2ZXJTZWFyY2ggPSB0b0Jvb2xlYW4odmFsdWUpO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56TW9kZSh2YWx1ZTogJ2RlZmF1bHQnIHwgJ211bHRpcGxlJyB8ICd0YWdzJykge1xuICAgIHRoaXMubnpTZWxlY3RTZXJ2aWNlLm1vZGUgPSB2YWx1ZTtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5jaGVjaygpO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56RmlsdGVyT3B0aW9uKHZhbHVlOiBURmlsdGVyT3B0aW9uKSB7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2UuZmlsdGVyT3B0aW9uID0gdmFsdWU7XG4gIH1cblxuICBASW5wdXQoKVxuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIHNldCBjb21wYXJlV2l0aCh2YWx1ZTogKG8xOiBhbnksIG8yOiBhbnkpID0+IGJvb2xlYW4pIHtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5jb21wYXJlV2l0aCA9IHZhbHVlO1xuICB9XG5cbiAgQElucHV0KClcbiAgc2V0IG56QXV0b0ZvY3VzKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5fYXV0b0ZvY3VzID0gdG9Cb29sZWFuKHZhbHVlKTtcbiAgICB0aGlzLnVwZGF0ZUF1dG9Gb2N1cygpO1xuICB9XG5cbiAgZ2V0IG56QXV0b0ZvY3VzKCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiB0aGlzLl9hdXRvRm9jdXM7XG4gIH1cblxuICBASW5wdXQoKVxuICBzZXQgbnpPcGVuKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5vcGVuID0gdmFsdWU7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2Uuc2V0T3BlblN0YXRlKHZhbHVlKTtcbiAgfVxuXG4gIEBJbnB1dCgpXG4gIHNldCBuekRpc2FibGVkKHZhbHVlOiBib29sZWFuKSB7XG4gICAgdGhpcy5fZGlzYWJsZWQgPSB0b0Jvb2xlYW4odmFsdWUpO1xuICAgIHRoaXMubnpTZWxlY3RTZXJ2aWNlLmRpc2FibGVkID0gdGhpcy5fZGlzYWJsZWQ7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2UuY2hlY2soKTtcbiAgICBpZiAodGhpcy5uekRpc2FibGVkICYmIHRoaXMuaXNJbml0KSB7XG4gICAgICB0aGlzLmNsb3NlRHJvcERvd24oKTtcbiAgICB9XG4gIH1cblxuICBnZXQgbnpEaXNhYmxlZCgpOiBib29sZWFuIHtcbiAgICByZXR1cm4gdGhpcy5fZGlzYWJsZWQ7XG4gIH1cblxuICB1cGRhdGVBdXRvRm9jdXMoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMubnpTZWxlY3RUb3BDb250cm9sQ29tcG9uZW50LmlucHV0RWxlbWVudCkge1xuICAgICAgaWYgKHRoaXMubnpBdXRvRm9jdXMpIHtcbiAgICAgICAgdGhpcy5yZW5kZXJlci5zZXRBdHRyaWJ1dGUoXG4gICAgICAgICAgdGhpcy5uelNlbGVjdFRvcENvbnRyb2xDb21wb25lbnQuaW5wdXRFbGVtZW50Lm5hdGl2ZUVsZW1lbnQsXG4gICAgICAgICAgJ2F1dG9mb2N1cycsXG4gICAgICAgICAgJ2F1dG9mb2N1cydcbiAgICAgICAgKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQXR0cmlidXRlKHRoaXMubnpTZWxlY3RUb3BDb250cm9sQ29tcG9uZW50LmlucHV0RWxlbWVudC5uYXRpdmVFbGVtZW50LCAnYXV0b2ZvY3VzJyk7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgZm9jdXMoKTogdm9pZCB7XG4gICAgaWYgKHRoaXMubnpTZWxlY3RUb3BDb250cm9sQ29tcG9uZW50LmlucHV0RWxlbWVudCkge1xuICAgICAgdGhpcy5mb2N1c01vbml0b3IuZm9jdXNWaWEodGhpcy5uelNlbGVjdFRvcENvbnRyb2xDb21wb25lbnQuaW5wdXRFbGVtZW50LCAna2V5Ym9hcmQnKTtcbiAgICAgIHRoaXMubnpGb2N1cy5lbWl0KCk7XG4gICAgfVxuICB9XG5cbiAgYmx1cigpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uelNlbGVjdFRvcENvbnRyb2xDb21wb25lbnQuaW5wdXRFbGVtZW50KSB7XG4gICAgICB0aGlzLm56U2VsZWN0VG9wQ29udHJvbENvbXBvbmVudC5pbnB1dEVsZW1lbnQubmF0aXZlRWxlbWVudC5ibHVyKCk7XG4gICAgICB0aGlzLm56Qmx1ci5lbWl0KCk7XG4gICAgfVxuICB9XG5cbiAgb25LZXlEb3duKGV2ZW50OiBLZXlib2FyZEV2ZW50KTogdm9pZCB7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2Uub25LZXlEb3duKGV2ZW50KTtcbiAgfVxuXG4gIHRvZ2dsZURyb3BEb3duKCk6IHZvaWQge1xuICAgIGlmICghdGhpcy5uekRpc2FibGVkKSB7XG4gICAgICB0aGlzLm56U2VsZWN0U2VydmljZS5zZXRPcGVuU3RhdGUoIXRoaXMub3Blbik7XG4gICAgfVxuICB9XG5cbiAgY2xvc2VEcm9wRG93bigpOiB2b2lkIHtcbiAgICB0aGlzLm56U2VsZWN0U2VydmljZS5zZXRPcGVuU3RhdGUoZmFsc2UpO1xuICB9XG5cbiAgb25Qb3NpdGlvbkNoYW5nZShwb3NpdGlvbjogQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9uQ2hhbmdlKTogdm9pZCB7XG4gICAgdGhpcy5kcm9wRG93blBvc2l0aW9uID0gcG9zaXRpb24uY29ubmVjdGlvblBhaXIub3JpZ2luWTtcbiAgfVxuXG4gIHVwZGF0ZUNka0Nvbm5lY3RlZE92ZXJsYXlTdGF0dXMoKTogdm9pZCB7XG4gICAgdGhpcy50cmlnZ2VyV2lkdGggPSB0aGlzLmNka092ZXJsYXlPcmlnaW4uZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LmdldEJvdW5kaW5nQ2xpZW50UmVjdCgpLndpZHRoO1xuICB9XG5cbiAgdXBkYXRlQ2RrQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9ucygpOiB2b2lkIHtcbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIGlmICh0aGlzLmNka0Nvbm5lY3RlZE92ZXJsYXkgJiYgdGhpcy5jZGtDb25uZWN0ZWRPdmVybGF5Lm92ZXJsYXlSZWYpIHtcbiAgICAgICAgdGhpcy5jZGtDb25uZWN0ZWRPdmVybGF5Lm92ZXJsYXlSZWYudXBkYXRlUG9zaXRpb24oKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgcmVuZGVyZXI6IFJlbmRlcmVyMixcbiAgICBwdWJsaWMgbnpTZWxlY3RTZXJ2aWNlOiBOelNlbGVjdFNlcnZpY2UsXG4gICAgcHJpdmF0ZSBjZHI6IENoYW5nZURldGVjdG9yUmVmLFxuICAgIHByaXZhdGUgZm9jdXNNb25pdG9yOiBGb2N1c01vbml0b3IsXG4gICAgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBASG9zdCgpIEBPcHRpb25hbCgpIHB1YmxpYyBub0FuaW1hdGlvbj86IE56Tm9BbmltYXRpb25EaXJlY3RpdmVcbiAgKSB7XG4gICAgcmVuZGVyZXIuYWRkQ2xhc3MoZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnYW50LXNlbGVjdCcpO1xuICB9XG5cbiAgLyoqIHVwZGF0ZSBuZ01vZGVsIC0+IHVwZGF0ZSBsaXN0T2ZTZWxlY3RlZFZhbHVlICoqL1xuICAvLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG4gIHdyaXRlVmFsdWUodmFsdWU6IGFueSB8IGFueVtdKTogdm9pZCB7XG4gICAgdGhpcy52YWx1ZSA9IHZhbHVlO1xuICAgIGxldCBsaXN0VmFsdWU6IGFueVtdID0gW107IC8vIHRzbGludDpkaXNhYmxlLWxpbmU6bm8tYW55XG4gICAgaWYgKGlzTm90TmlsKHZhbHVlKSkge1xuICAgICAgaWYgKEFycmF5LmlzQXJyYXkodmFsdWUpKSB7XG4gICAgICAgIGxpc3RWYWx1ZSA9IHZhbHVlO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgbGlzdFZhbHVlID0gW3ZhbHVlXTtcbiAgICAgIH1cbiAgICB9XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2UudXBkYXRlTGlzdE9mU2VsZWN0ZWRWYWx1ZShsaXN0VmFsdWUsIGZhbHNlKTtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHJlZ2lzdGVyT25DaGFuZ2UoZm46ICh2YWx1ZTogc3RyaW5nIHwgc3RyaW5nW10pID0+IHZvaWQpOiB2b2lkIHtcbiAgICB0aGlzLm9uQ2hhbmdlID0gZm47XG4gIH1cblxuICByZWdpc3Rlck9uVG91Y2hlZChmbjogKCkgPT4gdm9pZCk6IHZvaWQge1xuICAgIHRoaXMub25Ub3VjaGVkID0gZm47XG4gIH1cblxuICBzZXREaXNhYmxlZFN0YXRlKGlzRGlzYWJsZWQ6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLm56RGlzYWJsZWQgPSBpc0Rpc2FibGVkO1xuICAgIHRoaXMuY2RyLm1hcmtGb3JDaGVjaygpO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2Uuc2VhcmNoVmFsdWUkLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUoZGF0YSA9PiB7XG4gICAgICB0aGlzLm56T25TZWFyY2guZW1pdChkYXRhKTtcbiAgICAgIHRoaXMudXBkYXRlQ2RrQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9ucygpO1xuICAgIH0pO1xuICAgIHRoaXMubnpTZWxlY3RTZXJ2aWNlLm1vZGVsQ2hhbmdlJC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSkuc3Vic2NyaWJlKG1vZGVsVmFsdWUgPT4ge1xuICAgICAgaWYgKHRoaXMudmFsdWUgIT09IG1vZGVsVmFsdWUpIHtcbiAgICAgICAgdGhpcy52YWx1ZSA9IG1vZGVsVmFsdWU7XG4gICAgICAgIHRoaXMub25DaGFuZ2UodGhpcy52YWx1ZSk7XG4gICAgICAgIHRoaXMudXBkYXRlQ2RrQ29ubmVjdGVkT3ZlcmxheVBvc2l0aW9ucygpO1xuICAgICAgfVxuICAgIH0pO1xuICAgIHRoaXMubnpTZWxlY3RTZXJ2aWNlLm9wZW4kLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUodmFsdWUgPT4ge1xuICAgICAgaWYgKHRoaXMub3BlbiAhPT0gdmFsdWUpIHtcbiAgICAgICAgdGhpcy5uek9wZW5DaGFuZ2UuZW1pdCh2YWx1ZSk7XG4gICAgICB9XG4gICAgICBpZiAodmFsdWUpIHtcbiAgICAgICAgdGhpcy5mb2N1cygpO1xuICAgICAgICB0aGlzLnVwZGF0ZUNka0Nvbm5lY3RlZE92ZXJsYXlTdGF0dXMoKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuYmx1cigpO1xuICAgICAgICB0aGlzLm9uVG91Y2hlZCgpO1xuICAgICAgfVxuICAgICAgdGhpcy5vcGVuID0gdmFsdWU7XG4gICAgfSk7XG4gICAgdGhpcy5uelNlbGVjdFNlcnZpY2UuY2hlY2skLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgdGhpcy5jZHIubWFya0ZvckNoZWNrKCk7XG4gICAgfSk7XG4gIH1cblxuICBuZ0FmdGVyVmlld0luaXQoKTogdm9pZCB7XG4gICAgdGhpcy51cGRhdGVDZGtDb25uZWN0ZWRPdmVybGF5U3RhdHVzKCk7XG4gICAgdGhpcy5pc0luaXQgPSB0cnVlO1xuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIHRoaXMubGlzdE9mTnpPcHRpb25Hcm91cENvbXBvbmVudC5jaGFuZ2VzXG4gICAgICAucGlwZShcbiAgICAgICAgc3RhcnRXaXRoKHRydWUpLFxuICAgICAgICBmbGF0TWFwKCgpID0+XG4gICAgICAgICAgbWVyZ2UoXG4gICAgICAgICAgICB0aGlzLmxpc3RPZk56T3B0aW9uR3JvdXBDb21wb25lbnQuY2hhbmdlcyxcbiAgICAgICAgICAgIHRoaXMubGlzdE9mTnpPcHRpb25Db21wb25lbnQuY2hhbmdlcyxcbiAgICAgICAgICAgIC4uLnRoaXMubGlzdE9mTnpPcHRpb25Hcm91cENvbXBvbmVudC5tYXAoZ3JvdXAgPT5cbiAgICAgICAgICAgICAgZ3JvdXAubGlzdE9mTnpPcHRpb25Db21wb25lbnQgPyBncm91cC5saXN0T2ZOek9wdGlvbkNvbXBvbmVudC5jaGFuZ2VzIDogRU1QVFlcbiAgICAgICAgICAgIClcbiAgICAgICAgICApLnBpcGUoc3RhcnRXaXRoKHRydWUpKVxuICAgICAgICApXG4gICAgICApXG4gICAgICAuc3Vic2NyaWJlKCgpID0+IHtcbiAgICAgICAgdGhpcy5uelNlbGVjdFNlcnZpY2UudXBkYXRlVGVtcGxhdGVPcHRpb24oXG4gICAgICAgICAgdGhpcy5saXN0T2ZOek9wdGlvbkNvbXBvbmVudC50b0FycmF5KCksXG4gICAgICAgICAgdGhpcy5saXN0T2ZOek9wdGlvbkdyb3VwQ29tcG9uZW50LnRvQXJyYXkoKVxuICAgICAgICApO1xuICAgICAgfSk7XG4gIH1cblxuICBuZ09uRGVzdHJveSgpOiB2b2lkIHtcbiAgICB0aGlzLmRlc3Ryb3kkLm5leHQoKTtcbiAgICB0aGlzLmRlc3Ryb3kkLmNvbXBsZXRlKCk7XG4gIH1cbn1cbiJdfQ==