/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ElementRef, Input, Renderer2, ViewChild, ViewEncapsulation } from '@angular/core';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
/**
 * @record
 */
export function NzAvatarSizeMap() { }
var NzAvatarComponent = /** @class */ (function () {
    function NzAvatarComponent(elementRef, cd, updateHostClassService, renderer) {
        this.elementRef = elementRef;
        this.cd = cd;
        this.updateHostClassService = updateHostClassService;
        this.renderer = renderer;
        this.nzShape = 'circle';
        this.nzSize = 'default';
        this.oldAPIIcon = true; // Make the user defined icon compatible to old API. Should be removed in 2.0.
        // Make the user defined icon compatible to old API. Should be removed in 2.0.
        this.hasText = false;
        this.hasSrc = true;
        this.hasIcon = false;
        this.el = this.elementRef.nativeElement;
        this.prefixCls = 'ant-avatar';
        this.sizeMap = { large: 'lg', small: 'sm' };
    }
    /**
     * @template THIS
     * @this {THIS}
     * @return {THIS}
     */
    NzAvatarComponent.prototype.setClass = /**
     * @template THIS
     * @this {THIS}
     * @return {THIS}
     */
    function () {
        var _a;
        /** @type {?} */
        var classMap = (_a = {},
            _a[(/** @type {?} */ (this)).prefixCls] = true,
            _a[(/** @type {?} */ (this)).prefixCls + "-" + (/** @type {?} */ (this)).sizeMap[(/** @type {?} */ (this)).nzSize]] = (/** @type {?} */ (this)).sizeMap[(/** @type {?} */ (this)).nzSize],
            _a[(/** @type {?} */ (this)).prefixCls + "-" + (/** @type {?} */ (this)).nzShape] = (/** @type {?} */ (this)).nzShape,
            _a[(/** @type {?} */ (this)).prefixCls + "-icon"] = (/** @type {?} */ (this)).nzIcon,
            _a[(/** @type {?} */ (this)).prefixCls + "-image"] = (/** @type {?} */ (this)).hasSrc // downgrade after image error
        ,
            _a);
        (/** @type {?} */ (this)).updateHostClassService.updateHostClass((/** @type {?} */ (this)).el, classMap);
        (/** @type {?} */ (this)).cd.detectChanges();
        return (/** @type {?} */ (this));
    };
    /**
     * @return {?}
     */
    NzAvatarComponent.prototype.imgError = /**
     * @return {?}
     */
    function () {
        this.hasSrc = false;
        this.hasIcon = false;
        this.hasText = false;
        if (this.nzIcon) {
            this.hasIcon = true;
        }
        else if (this.nzText) {
            this.hasText = true;
        }
        this.setClass().notifyCalc();
        this.setSizeStyle();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzAvatarComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        if (changes.hasOwnProperty('nzIcon') && changes.nzIcon.currentValue) {
            this.oldAPIIcon = changes.nzIcon.currentValue.indexOf('anticon') > -1;
        }
        this.hasText = !this.nzSrc && !!this.nzText;
        this.hasIcon = !this.nzSrc && !!this.nzIcon;
        this.hasSrc = !!this.nzSrc;
        this.setClass().notifyCalc();
        this.setSizeStyle();
    };
    /**
     * @private
     * @return {?}
     */
    NzAvatarComponent.prototype.calcStringSize = /**
     * @private
     * @return {?}
     */
    function () {
        if (!this.hasText) {
            return;
        }
        /** @type {?} */
        var childrenWidth = this.textEl.nativeElement.offsetWidth;
        /** @type {?} */
        var avatarWidth = this.el.getBoundingClientRect().width;
        /** @type {?} */
        var scale = avatarWidth - 8 < childrenWidth ? (avatarWidth - 8) / childrenWidth : 1;
        this.textStyles = {
            transform: "scale(" + scale + ") translateX(-50%)"
        };
        if (typeof this.nzSize === 'number') {
            Object.assign(this.textStyles, {
                lineHeight: this.nzSize + "px"
            });
        }
        this.cd.detectChanges();
    };
    /**
     * @private
     * @template THIS
     * @this {THIS}
     * @return {THIS}
     */
    NzAvatarComponent.prototype.notifyCalc = /**
     * @private
     * @template THIS
     * @this {THIS}
     * @return {THIS}
     */
    function () {
        var _this = this;
        // If use ngAfterViewChecked, always demands more computations, so......
        setTimeout((/**
         * @return {?}
         */
        function () {
            (/** @type {?} */ (_this)).calcStringSize();
        }));
        return (/** @type {?} */ (this));
    };
    /**
     * @private
     * @return {?}
     */
    NzAvatarComponent.prototype.setSizeStyle = /**
     * @private
     * @return {?}
     */
    function () {
        if (typeof this.nzSize === 'string') {
            return;
        }
        this.renderer.setStyle(this.el, 'width', this.nzSize + "px");
        this.renderer.setStyle(this.el, 'height', this.nzSize + "px");
        this.renderer.setStyle(this.el, 'line-height', this.nzSize + "px");
        if (this.hasIcon) {
            this.renderer.setStyle(this.el, 'font-size', this.nzSize / 2 + "px");
        }
    };
    NzAvatarComponent.decorators = [
        { type: Component, args: [{
                    selector: 'nz-avatar',
                    template: "<i nz-icon *ngIf=\"nzIcon && hasIcon\" [type]=\"!oldAPIIcon && nzIcon\" [ngClass]=\"oldAPIIcon && nzIcon\"></i>\n<img [src]=\"nzSrc\" *ngIf=\"nzSrc && hasSrc\" (error)=\"imgError()\"/>\n<span class=\"ant-avatar-string\" #textEl [ngStyle]=\"textStyles\" *ngIf=\"nzText && hasText\">{{ nzText }}</span>",
                    providers: [NzUpdateHostClassService],
                    preserveWhitespaces: false,
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None
                }] }
    ];
    /** @nocollapse */
    NzAvatarComponent.ctorParameters = function () { return [
        { type: ElementRef },
        { type: ChangeDetectorRef },
        { type: NzUpdateHostClassService },
        { type: Renderer2 }
    ]; };
    NzAvatarComponent.propDecorators = {
        nzShape: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzText: [{ type: Input }],
        nzSrc: [{ type: Input }],
        nzIcon: [{ type: Input }],
        textEl: [{ type: ViewChild, args: ['textEl',] }]
    };
    return NzAvatarComponent;
}());
export { NzAvatarComponent };
if (false) {
    /** @type {?} */
    NzAvatarComponent.prototype.nzShape;
    /** @type {?} */
    NzAvatarComponent.prototype.nzSize;
    /** @type {?} */
    NzAvatarComponent.prototype.nzText;
    /** @type {?} */
    NzAvatarComponent.prototype.nzSrc;
    /** @type {?} */
    NzAvatarComponent.prototype.nzIcon;
    /** @type {?} */
    NzAvatarComponent.prototype.oldAPIIcon;
    /** @type {?} */
    NzAvatarComponent.prototype.hasText;
    /** @type {?} */
    NzAvatarComponent.prototype.hasSrc;
    /** @type {?} */
    NzAvatarComponent.prototype.hasIcon;
    /** @type {?} */
    NzAvatarComponent.prototype.textStyles;
    /** @type {?} */
    NzAvatarComponent.prototype.textEl;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.el;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.prefixCls;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.sizeMap;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.cd;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.updateHostClassService;
    /**
     * @type {?}
     * @private
     */
    NzAvatarComponent.prototype.renderer;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYXZhdGFyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJhdmF0YXIvbnotYXZhdGFyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBRUwsU0FBUyxFQUVULFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLHdCQUF3QixFQUFFLE1BQU0sNENBQTRDLENBQUM7Ozs7QUFNdEYscUNBRUM7QUFFRDtJQTJCRSwyQkFDVSxVQUFzQixFQUN0QixFQUFxQixFQUNyQixzQkFBZ0QsRUFDaEQsUUFBbUI7UUFIbkIsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0QixPQUFFLEdBQUYsRUFBRSxDQUFtQjtRQUNyQiwyQkFBc0IsR0FBdEIsc0JBQXNCLENBQTBCO1FBQ2hELGFBQVEsR0FBUixRQUFRLENBQVc7UUF0QnBCLFlBQU8sR0FBa0IsUUFBUSxDQUFDO1FBQ2xDLFdBQU0sR0FBaUIsU0FBUyxDQUFDO1FBSzFDLGVBQVUsR0FBRyxJQUFJLENBQUMsQ0FBQyw4RUFBOEU7O1FBQ2pHLFlBQU8sR0FBWSxLQUFLLENBQUM7UUFDekIsV0FBTSxHQUFZLElBQUksQ0FBQztRQUN2QixZQUFPLEdBQVksS0FBSyxDQUFDO1FBS2pCLE9BQUUsR0FBZ0IsSUFBSSxDQUFDLFVBQVUsQ0FBQyxhQUFhLENBQUM7UUFDaEQsY0FBUyxHQUFHLFlBQVksQ0FBQztRQUN6QixZQUFPLEdBQWMsRUFBRSxLQUFLLEVBQUUsSUFBSSxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsQ0FBQztJQU92RCxDQUFDOzs7Ozs7SUFFSixvQ0FBUTs7Ozs7SUFBUjs7O1lBQ1EsUUFBUTtZQUNaLEdBQUMsbUJBQUEsSUFBSSxFQUFBLENBQUMsU0FBUyxJQUFHLElBQUk7WUFDdEIsR0FBSSxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLFNBQUksbUJBQUEsSUFBSSxFQUFBLENBQUMsT0FBTyxDQUFDLG1CQUFBLElBQUksRUFBQSxDQUFDLE1BQU0sQ0FBRyxJQUFHLG1CQUFBLElBQUksRUFBQSxDQUFDLE9BQU8sQ0FBQyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxNQUFNLENBQUM7WUFDN0UsR0FBSSxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLFNBQUksbUJBQUEsSUFBSSxFQUFBLENBQUMsT0FBUyxJQUFHLG1CQUFBLElBQUksRUFBQSxDQUFDLE9BQU87WUFDbkQsR0FBSSxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLFVBQU8sSUFBRyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxNQUFNO1lBQ3ZDLEdBQUksbUJBQUEsSUFBSSxFQUFBLENBQUMsU0FBUyxXQUFRLElBQUcsbUJBQUEsSUFBSSxFQUFBLENBQUMsTUFBTSxDQUFDLDhCQUE4Qjs7ZUFDeEU7UUFDRCxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxzQkFBc0IsQ0FBQyxlQUFlLENBQUMsbUJBQUEsSUFBSSxFQUFBLENBQUMsRUFBRSxFQUFFLFFBQVEsQ0FBQyxDQUFDO1FBQy9ELG1CQUFBLElBQUksRUFBQSxDQUFDLEVBQUUsQ0FBQyxhQUFhLEVBQUUsQ0FBQztRQUN4QixPQUFPLG1CQUFBLElBQUksRUFBQSxDQUFDO0lBQ2QsQ0FBQzs7OztJQUVELG9DQUFROzs7SUFBUjtRQUNFLElBQUksQ0FBQyxNQUFNLEdBQUcsS0FBSyxDQUFDO1FBQ3BCLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDO1FBQ3JCLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDO1FBQ3JCLElBQUksSUFBSSxDQUFDLE1BQU0sRUFBRTtZQUNmLElBQUksQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDO1NBQ3JCO2FBQU0sSUFBSSxJQUFJLENBQUMsTUFBTSxFQUFFO1lBQ3RCLElBQUksQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDO1NBQ3JCO1FBQ0QsSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDLFVBQVUsRUFBRSxDQUFDO1FBQzdCLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUN0QixDQUFDOzs7OztJQUVELHVDQUFXOzs7O0lBQVgsVUFBWSxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxjQUFjLENBQUMsUUFBUSxDQUFDLElBQUksT0FBTyxDQUFDLE1BQU0sQ0FBQyxZQUFZLEVBQUU7WUFDbkUsSUFBSSxDQUFDLFVBQVUsR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDLFlBQVksQ0FBQyxPQUFPLENBQUMsU0FBUyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUM7U0FDdkU7UUFDRCxJQUFJLENBQUMsT0FBTyxHQUFHLENBQUMsSUFBSSxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQztRQUM1QyxJQUFJLENBQUMsT0FBTyxHQUFHLENBQUMsSUFBSSxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQztRQUM1QyxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDO1FBRTNCLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQyxVQUFVLEVBQUUsQ0FBQztRQUM3QixJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDdEIsQ0FBQzs7Ozs7SUFFTywwQ0FBYzs7OztJQUF0QjtRQUNFLElBQUksQ0FBQyxJQUFJLENBQUMsT0FBTyxFQUFFO1lBQ2pCLE9BQU87U0FDUjs7WUFFSyxhQUFhLEdBQUcsSUFBSSxDQUFDLE1BQU0sQ0FBQyxhQUFhLENBQUMsV0FBVzs7WUFDckQsV0FBVyxHQUFHLElBQUksQ0FBQyxFQUFFLENBQUMscUJBQXFCLEVBQUUsQ0FBQyxLQUFLOztZQUNuRCxLQUFLLEdBQUcsV0FBVyxHQUFHLENBQUMsR0FBRyxhQUFhLENBQUMsQ0FBQyxDQUFDLENBQUMsV0FBVyxHQUFHLENBQUMsQ0FBQyxHQUFHLGFBQWEsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNyRixJQUFJLENBQUMsVUFBVSxHQUFHO1lBQ2hCLFNBQVMsRUFBRSxXQUFTLEtBQUssdUJBQW9CO1NBQzlDLENBQUM7UUFDRixJQUFJLE9BQU8sSUFBSSxDQUFDLE1BQU0sS0FBSyxRQUFRLEVBQUU7WUFDbkMsTUFBTSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsVUFBVSxFQUFFO2dCQUM3QixVQUFVLEVBQUssSUFBSSxDQUFDLE1BQU0sT0FBSTthQUMvQixDQUFDLENBQUM7U0FDSjtRQUNELElBQUksQ0FBQyxFQUFFLENBQUMsYUFBYSxFQUFFLENBQUM7SUFDMUIsQ0FBQzs7Ozs7OztJQUVPLHNDQUFVOzs7Ozs7SUFBbEI7UUFBQSxpQkFNQztRQUxDLHdFQUF3RTtRQUN4RSxVQUFVOzs7UUFBQztZQUNULG1CQUFBLEtBQUksRUFBQSxDQUFDLGNBQWMsRUFBRSxDQUFDO1FBQ3hCLENBQUMsRUFBQyxDQUFDO1FBQ0gsT0FBTyxtQkFBQSxJQUFJLEVBQUEsQ0FBQztJQUNkLENBQUM7Ozs7O0lBRU8sd0NBQVk7Ozs7SUFBcEI7UUFDRSxJQUFJLE9BQU8sSUFBSSxDQUFDLE1BQU0sS0FBSyxRQUFRLEVBQUU7WUFDbkMsT0FBTztTQUNSO1FBQ0QsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSxPQUFPLEVBQUssSUFBSSxDQUFDLE1BQU0sT0FBSSxDQUFDLENBQUM7UUFDN0QsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSxRQUFRLEVBQUssSUFBSSxDQUFDLE1BQU0sT0FBSSxDQUFDLENBQUM7UUFDOUQsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSxhQUFhLEVBQUssSUFBSSxDQUFDLE1BQU0sT0FBSSxDQUFDLENBQUM7UUFDbkUsSUFBSSxJQUFJLENBQUMsT0FBTyxFQUFFO1lBQ2hCLElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxFQUFFLEVBQUUsV0FBVyxFQUFLLElBQUksQ0FBQyxNQUFNLEdBQUcsQ0FBQyxPQUFJLENBQUMsQ0FBQztTQUN0RTtJQUNILENBQUM7O2dCQTdHRixTQUFTLFNBQUM7b0JBQ1QsUUFBUSxFQUFFLFdBQVc7b0JBQ3JCLHdUQUF5QztvQkFDekMsU0FBUyxFQUFFLENBQUMsd0JBQXdCLENBQUM7b0JBQ3JDLG1CQUFtQixFQUFFLEtBQUs7b0JBQzFCLGVBQWUsRUFBRSx1QkFBdUIsQ0FBQyxNQUFNO29CQUMvQyxhQUFhLEVBQUUsaUJBQWlCLENBQUMsSUFBSTtpQkFDdEM7Ozs7Z0JBMUJDLFVBQVU7Z0JBRlYsaUJBQWlCO2dCQVdWLHdCQUF3QjtnQkFOL0IsU0FBUzs7OzBCQXlCUixLQUFLO3lCQUNMLEtBQUs7eUJBQ0wsS0FBSzt3QkFDTCxLQUFLO3lCQUNMLEtBQUs7eUJBUUwsU0FBUyxTQUFDLFFBQVE7O0lBeUZyQix3QkFBQztDQUFBLEFBOUdELElBOEdDO1NBdEdZLGlCQUFpQjs7O0lBQzVCLG9DQUEyQzs7SUFDM0MsbUNBQTBDOztJQUMxQyxtQ0FBd0I7O0lBQ3hCLGtDQUF1Qjs7SUFDdkIsbUNBQXdCOztJQUV4Qix1Q0FBa0I7O0lBQ2xCLG9DQUF5Qjs7SUFDekIsbUNBQXVCOztJQUN2QixvQ0FBeUI7O0lBQ3pCLHVDQUFlOztJQUVmLG1DQUF3Qzs7Ozs7SUFFeEMsK0JBQXdEOzs7OztJQUN4RCxzQ0FBaUM7Ozs7O0lBQ2pDLG9DQUEwRDs7Ozs7SUFHeEQsdUNBQThCOzs7OztJQUM5QiwrQkFBNkI7Ozs7O0lBQzdCLG1EQUF3RDs7Ozs7SUFDeEQscUNBQTJCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3ksXG4gIENoYW5nZURldGVjdG9yUmVmLFxuICBDb21wb25lbnQsXG4gIEVsZW1lbnRSZWYsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIFJlbmRlcmVyMixcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IE56U2l6ZUxEU1R5cGUsIE56U2l6ZU1hcCB9IGZyb20gJy4uL2NvcmUvdHlwZXMvc2l6ZSc7XG5cbmV4cG9ydCB0eXBlIE56QXZhdGFyU2hhcGUgPSAnc3F1YXJlJyB8ICdjaXJjbGUnO1xuZXhwb3J0IHR5cGUgTnpBdmF0YXJTaXplID0gTnpTaXplTERTVHlwZSB8IG51bWJlcjtcblxuZXhwb3J0IGludGVyZmFjZSBOekF2YXRhclNpemVNYXAge1xuICBbc2l6ZTogc3RyaW5nXTogc3RyaW5nO1xufVxuXG5AQ29tcG9uZW50KHtcbiAgc2VsZWN0b3I6ICduei1hdmF0YXInLFxuICB0ZW1wbGF0ZVVybDogJy4vbnotYXZhdGFyLmNvbXBvbmVudC5odG1sJyxcbiAgcHJvdmlkZXJzOiBbTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlXSxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIGNoYW5nZURldGVjdGlvbjogQ2hhbmdlRGV0ZWN0aW9uU3RyYXRlZ3kuT25QdXNoLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lXG59KVxuZXhwb3J0IGNsYXNzIE56QXZhdGFyQ29tcG9uZW50IGltcGxlbWVudHMgT25DaGFuZ2VzIHtcbiAgQElucHV0KCkgbnpTaGFwZTogTnpBdmF0YXJTaGFwZSA9ICdjaXJjbGUnO1xuICBASW5wdXQoKSBuelNpemU6IE56QXZhdGFyU2l6ZSA9ICdkZWZhdWx0JztcbiAgQElucHV0KCkgbnpUZXh0OiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56U3JjOiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56SWNvbjogc3RyaW5nO1xuXG4gIG9sZEFQSUljb24gPSB0cnVlOyAvLyBNYWtlIHRoZSB1c2VyIGRlZmluZWQgaWNvbiBjb21wYXRpYmxlIHRvIG9sZCBBUEkuIFNob3VsZCBiZSByZW1vdmVkIGluIDIuMC5cbiAgaGFzVGV4dDogYm9vbGVhbiA9IGZhbHNlO1xuICBoYXNTcmM6IGJvb2xlYW4gPSB0cnVlO1xuICBoYXNJY29uOiBib29sZWFuID0gZmFsc2U7XG4gIHRleHRTdHlsZXM6IHt9O1xuXG4gIEBWaWV3Q2hpbGQoJ3RleHRFbCcpIHRleHRFbDogRWxlbWVudFJlZjtcblxuICBwcml2YXRlIGVsOiBIVE1MRWxlbWVudCA9IHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50O1xuICBwcml2YXRlIHByZWZpeENscyA9ICdhbnQtYXZhdGFyJztcbiAgcHJpdmF0ZSBzaXplTWFwOiBOelNpemVNYXAgPSB7IGxhcmdlOiAnbGcnLCBzbWFsbDogJ3NtJyB9O1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwcml2YXRlIGNkOiBDaGFuZ2VEZXRlY3RvclJlZixcbiAgICBwcml2YXRlIHVwZGF0ZUhvc3RDbGFzc1NlcnZpY2U6IE56VXBkYXRlSG9zdENsYXNzU2VydmljZSxcbiAgICBwcml2YXRlIHJlbmRlcmVyOiBSZW5kZXJlcjJcbiAgKSB7fVxuXG4gIHNldENsYXNzKCk6IHRoaXMge1xuICAgIGNvbnN0IGNsYXNzTWFwID0ge1xuICAgICAgW3RoaXMucHJlZml4Q2xzXTogdHJ1ZSxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tJHt0aGlzLnNpemVNYXBbdGhpcy5uelNpemVdfWBdOiB0aGlzLnNpemVNYXBbdGhpcy5uelNpemVdLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS0ke3RoaXMubnpTaGFwZX1gXTogdGhpcy5uelNoYXBlLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1pY29uYF06IHRoaXMubnpJY29uLFxuICAgICAgW2Ake3RoaXMucHJlZml4Q2xzfS1pbWFnZWBdOiB0aGlzLmhhc1NyYyAvLyBkb3duZ3JhZGUgYWZ0ZXIgaW1hZ2UgZXJyb3JcbiAgICB9O1xuICAgIHRoaXMudXBkYXRlSG9zdENsYXNzU2VydmljZS51cGRhdGVIb3N0Q2xhc3ModGhpcy5lbCwgY2xhc3NNYXApO1xuICAgIHRoaXMuY2QuZGV0ZWN0Q2hhbmdlcygpO1xuICAgIHJldHVybiB0aGlzO1xuICB9XG5cbiAgaW1nRXJyb3IoKTogdm9pZCB7XG4gICAgdGhpcy5oYXNTcmMgPSBmYWxzZTtcbiAgICB0aGlzLmhhc0ljb24gPSBmYWxzZTtcbiAgICB0aGlzLmhhc1RleHQgPSBmYWxzZTtcbiAgICBpZiAodGhpcy5uekljb24pIHtcbiAgICAgIHRoaXMuaGFzSWNvbiA9IHRydWU7XG4gICAgfSBlbHNlIGlmICh0aGlzLm56VGV4dCkge1xuICAgICAgdGhpcy5oYXNUZXh0ID0gdHJ1ZTtcbiAgICB9XG4gICAgdGhpcy5zZXRDbGFzcygpLm5vdGlmeUNhbGMoKTtcbiAgICB0aGlzLnNldFNpemVTdHlsZSgpO1xuICB9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmIChjaGFuZ2VzLmhhc093blByb3BlcnR5KCduekljb24nKSAmJiBjaGFuZ2VzLm56SWNvbi5jdXJyZW50VmFsdWUpIHtcbiAgICAgIHRoaXMub2xkQVBJSWNvbiA9IGNoYW5nZXMubnpJY29uLmN1cnJlbnRWYWx1ZS5pbmRleE9mKCdhbnRpY29uJykgPiAtMTtcbiAgICB9XG4gICAgdGhpcy5oYXNUZXh0ID0gIXRoaXMubnpTcmMgJiYgISF0aGlzLm56VGV4dDtcbiAgICB0aGlzLmhhc0ljb24gPSAhdGhpcy5uelNyYyAmJiAhIXRoaXMubnpJY29uO1xuICAgIHRoaXMuaGFzU3JjID0gISF0aGlzLm56U3JjO1xuXG4gICAgdGhpcy5zZXRDbGFzcygpLm5vdGlmeUNhbGMoKTtcbiAgICB0aGlzLnNldFNpemVTdHlsZSgpO1xuICB9XG5cbiAgcHJpdmF0ZSBjYWxjU3RyaW5nU2l6ZSgpOiB2b2lkIHtcbiAgICBpZiAoIXRoaXMuaGFzVGV4dCkge1xuICAgICAgcmV0dXJuO1xuICAgIH1cblxuICAgIGNvbnN0IGNoaWxkcmVuV2lkdGggPSB0aGlzLnRleHRFbC5uYXRpdmVFbGVtZW50Lm9mZnNldFdpZHRoO1xuICAgIGNvbnN0IGF2YXRhcldpZHRoID0gdGhpcy5lbC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKS53aWR0aDtcbiAgICBjb25zdCBzY2FsZSA9IGF2YXRhcldpZHRoIC0gOCA8IGNoaWxkcmVuV2lkdGggPyAoYXZhdGFyV2lkdGggLSA4KSAvIGNoaWxkcmVuV2lkdGggOiAxO1xuICAgIHRoaXMudGV4dFN0eWxlcyA9IHtcbiAgICAgIHRyYW5zZm9ybTogYHNjYWxlKCR7c2NhbGV9KSB0cmFuc2xhdGVYKC01MCUpYFxuICAgIH07XG4gICAgaWYgKHR5cGVvZiB0aGlzLm56U2l6ZSA9PT0gJ251bWJlcicpIHtcbiAgICAgIE9iamVjdC5hc3NpZ24odGhpcy50ZXh0U3R5bGVzLCB7XG4gICAgICAgIGxpbmVIZWlnaHQ6IGAke3RoaXMubnpTaXplfXB4YFxuICAgICAgfSk7XG4gICAgfVxuICAgIHRoaXMuY2QuZGV0ZWN0Q2hhbmdlcygpO1xuICB9XG5cbiAgcHJpdmF0ZSBub3RpZnlDYWxjKCk6IHRoaXMge1xuICAgIC8vIElmIHVzZSBuZ0FmdGVyVmlld0NoZWNrZWQsIGFsd2F5cyBkZW1hbmRzIG1vcmUgY29tcHV0YXRpb25zLCBzby4uLi4uLlxuICAgIHNldFRpbWVvdXQoKCkgPT4ge1xuICAgICAgdGhpcy5jYWxjU3RyaW5nU2l6ZSgpO1xuICAgIH0pO1xuICAgIHJldHVybiB0aGlzO1xuICB9XG5cbiAgcHJpdmF0ZSBzZXRTaXplU3R5bGUoKTogdm9pZCB7XG4gICAgaWYgKHR5cGVvZiB0aGlzLm56U2l6ZSA9PT0gJ3N0cmluZycpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG4gICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsLCAnd2lkdGgnLCBgJHt0aGlzLm56U2l6ZX1weGApO1xuICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ2hlaWdodCcsIGAke3RoaXMubnpTaXplfXB4YCk7XG4gICAgdGhpcy5yZW5kZXJlci5zZXRTdHlsZSh0aGlzLmVsLCAnbGluZS1oZWlnaHQnLCBgJHt0aGlzLm56U2l6ZX1weGApO1xuICAgIGlmICh0aGlzLmhhc0ljb24pIHtcbiAgICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ2ZvbnQtc2l6ZScsIGAke3RoaXMubnpTaXplIC8gMn1weGApO1xuICAgIH1cbiAgfVxufVxuIl19