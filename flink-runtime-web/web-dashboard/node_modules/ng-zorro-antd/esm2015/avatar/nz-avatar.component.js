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
export class NzAvatarComponent {
    /**
     * @param {?} elementRef
     * @param {?} cd
     * @param {?} updateHostClassService
     * @param {?} renderer
     */
    constructor(elementRef, cd, updateHostClassService, renderer) {
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
    setClass() {
        /** @type {?} */
        const classMap = {
            [(/** @type {?} */ (this)).prefixCls]: true,
            [`${(/** @type {?} */ (this)).prefixCls}-${(/** @type {?} */ (this)).sizeMap[(/** @type {?} */ (this)).nzSize]}`]: (/** @type {?} */ (this)).sizeMap[(/** @type {?} */ (this)).nzSize],
            [`${(/** @type {?} */ (this)).prefixCls}-${(/** @type {?} */ (this)).nzShape}`]: (/** @type {?} */ (this)).nzShape,
            [`${(/** @type {?} */ (this)).prefixCls}-icon`]: (/** @type {?} */ (this)).nzIcon,
            [`${(/** @type {?} */ (this)).prefixCls}-image`]: (/** @type {?} */ (this)).hasSrc // downgrade after image error
        };
        (/** @type {?} */ (this)).updateHostClassService.updateHostClass((/** @type {?} */ (this)).el, classMap);
        (/** @type {?} */ (this)).cd.detectChanges();
        return (/** @type {?} */ (this));
    }
    /**
     * @return {?}
     */
    imgError() {
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
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.hasOwnProperty('nzIcon') && changes.nzIcon.currentValue) {
            this.oldAPIIcon = changes.nzIcon.currentValue.indexOf('anticon') > -1;
        }
        this.hasText = !this.nzSrc && !!this.nzText;
        this.hasIcon = !this.nzSrc && !!this.nzIcon;
        this.hasSrc = !!this.nzSrc;
        this.setClass().notifyCalc();
        this.setSizeStyle();
    }
    /**
     * @private
     * @return {?}
     */
    calcStringSize() {
        if (!this.hasText) {
            return;
        }
        /** @type {?} */
        const childrenWidth = this.textEl.nativeElement.offsetWidth;
        /** @type {?} */
        const avatarWidth = this.el.getBoundingClientRect().width;
        /** @type {?} */
        const scale = avatarWidth - 8 < childrenWidth ? (avatarWidth - 8) / childrenWidth : 1;
        this.textStyles = {
            transform: `scale(${scale}) translateX(-50%)`
        };
        if (typeof this.nzSize === 'number') {
            Object.assign(this.textStyles, {
                lineHeight: `${this.nzSize}px`
            });
        }
        this.cd.detectChanges();
    }
    /**
     * @private
     * @template THIS
     * @this {THIS}
     * @return {THIS}
     */
    notifyCalc() {
        // If use ngAfterViewChecked, always demands more computations, so......
        setTimeout((/**
         * @return {?}
         */
        () => {
            (/** @type {?} */ (this)).calcStringSize();
        }));
        return (/** @type {?} */ (this));
    }
    /**
     * @private
     * @return {?}
     */
    setSizeStyle() {
        if (typeof this.nzSize === 'string') {
            return;
        }
        this.renderer.setStyle(this.el, 'width', `${this.nzSize}px`);
        this.renderer.setStyle(this.el, 'height', `${this.nzSize}px`);
        this.renderer.setStyle(this.el, 'line-height', `${this.nzSize}px`);
        if (this.hasIcon) {
            this.renderer.setStyle(this.el, 'font-size', `${this.nzSize / 2}px`);
        }
    }
}
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
NzAvatarComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: ChangeDetectorRef },
    { type: NzUpdateHostClassService },
    { type: Renderer2 }
];
NzAvatarComponent.propDecorators = {
    nzShape: [{ type: Input }],
    nzSize: [{ type: Input }],
    nzText: [{ type: Input }],
    nzSrc: [{ type: Input }],
    nzIcon: [{ type: Input }],
    textEl: [{ type: ViewChild, args: ['textEl',] }]
};
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotYXZhdGFyLmNvbXBvbmVudC5qcyIsInNvdXJjZVJvb3QiOiJuZzovL25nLXpvcnJvLWFudGQvIiwic291cmNlcyI6WyJhdmF0YXIvbnotYXZhdGFyLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUNMLHVCQUF1QixFQUN2QixpQkFBaUIsRUFDakIsU0FBUyxFQUNULFVBQVUsRUFDVixLQUFLLEVBRUwsU0FBUyxFQUVULFNBQVMsRUFDVCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLHdCQUF3QixFQUFFLE1BQU0sNENBQTRDLENBQUM7Ozs7QUFNdEYscUNBRUM7QUFVRCxNQUFNLE9BQU8saUJBQWlCOzs7Ozs7O0lBbUI1QixZQUNVLFVBQXNCLEVBQ3RCLEVBQXFCLEVBQ3JCLHNCQUFnRCxFQUNoRCxRQUFtQjtRQUhuQixlQUFVLEdBQVYsVUFBVSxDQUFZO1FBQ3RCLE9BQUUsR0FBRixFQUFFLENBQW1CO1FBQ3JCLDJCQUFzQixHQUF0QixzQkFBc0IsQ0FBMEI7UUFDaEQsYUFBUSxHQUFSLFFBQVEsQ0FBVztRQXRCcEIsWUFBTyxHQUFrQixRQUFRLENBQUM7UUFDbEMsV0FBTSxHQUFpQixTQUFTLENBQUM7UUFLMUMsZUFBVSxHQUFHLElBQUksQ0FBQyxDQUFDLDhFQUE4RTs7UUFDakcsWUFBTyxHQUFZLEtBQUssQ0FBQztRQUN6QixXQUFNLEdBQVksSUFBSSxDQUFDO1FBQ3ZCLFlBQU8sR0FBWSxLQUFLLENBQUM7UUFLakIsT0FBRSxHQUFnQixJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsQ0FBQztRQUNoRCxjQUFTLEdBQUcsWUFBWSxDQUFDO1FBQ3pCLFlBQU8sR0FBYyxFQUFFLEtBQUssRUFBRSxJQUFJLEVBQUUsS0FBSyxFQUFFLElBQUksRUFBRSxDQUFDO0lBT3ZELENBQUM7Ozs7OztJQUVKLFFBQVE7O2NBQ0EsUUFBUSxHQUFHO1lBQ2YsQ0FBQyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLENBQUMsRUFBRSxJQUFJO1lBQ3RCLENBQUMsR0FBRyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLElBQUksbUJBQUEsSUFBSSxFQUFBLENBQUMsT0FBTyxDQUFDLG1CQUFBLElBQUksRUFBQSxDQUFDLE1BQU0sQ0FBQyxFQUFFLENBQUMsRUFBRSxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxPQUFPLENBQUMsbUJBQUEsSUFBSSxFQUFBLENBQUMsTUFBTSxDQUFDO1lBQzdFLENBQUMsR0FBRyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLElBQUksbUJBQUEsSUFBSSxFQUFBLENBQUMsT0FBTyxFQUFFLENBQUMsRUFBRSxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxPQUFPO1lBQ25ELENBQUMsR0FBRyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxTQUFTLE9BQU8sQ0FBQyxFQUFFLG1CQUFBLElBQUksRUFBQSxDQUFDLE1BQU07WUFDdkMsQ0FBQyxHQUFHLG1CQUFBLElBQUksRUFBQSxDQUFDLFNBQVMsUUFBUSxDQUFDLEVBQUUsbUJBQUEsSUFBSSxFQUFBLENBQUMsTUFBTSxDQUFDLDhCQUE4QjtTQUN4RTtRQUNELG1CQUFBLElBQUksRUFBQSxDQUFDLHNCQUFzQixDQUFDLGVBQWUsQ0FBQyxtQkFBQSxJQUFJLEVBQUEsQ0FBQyxFQUFFLEVBQUUsUUFBUSxDQUFDLENBQUM7UUFDL0QsbUJBQUEsSUFBSSxFQUFBLENBQUMsRUFBRSxDQUFDLGFBQWEsRUFBRSxDQUFDO1FBQ3hCLE9BQU8sbUJBQUEsSUFBSSxFQUFBLENBQUM7SUFDZCxDQUFDOzs7O0lBRUQsUUFBUTtRQUNOLElBQUksQ0FBQyxNQUFNLEdBQUcsS0FBSyxDQUFDO1FBQ3BCLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDO1FBQ3JCLElBQUksQ0FBQyxPQUFPLEdBQUcsS0FBSyxDQUFDO1FBQ3JCLElBQUksSUFBSSxDQUFDLE1BQU0sRUFBRTtZQUNmLElBQUksQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDO1NBQ3JCO2FBQU0sSUFBSSxJQUFJLENBQUMsTUFBTSxFQUFFO1lBQ3RCLElBQUksQ0FBQyxPQUFPLEdBQUcsSUFBSSxDQUFDO1NBQ3JCO1FBQ0QsSUFBSSxDQUFDLFFBQVEsRUFBRSxDQUFDLFVBQVUsRUFBRSxDQUFDO1FBQzdCLElBQUksQ0FBQyxZQUFZLEVBQUUsQ0FBQztJQUN0QixDQUFDOzs7OztJQUVELFdBQVcsQ0FBQyxPQUFzQjtRQUNoQyxJQUFJLE9BQU8sQ0FBQyxjQUFjLENBQUMsUUFBUSxDQUFDLElBQUksT0FBTyxDQUFDLE1BQU0sQ0FBQyxZQUFZLEVBQUU7WUFDbkUsSUFBSSxDQUFDLFVBQVUsR0FBRyxPQUFPLENBQUMsTUFBTSxDQUFDLFlBQVksQ0FBQyxPQUFPLENBQUMsU0FBUyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUM7U0FDdkU7UUFDRCxJQUFJLENBQUMsT0FBTyxHQUFHLENBQUMsSUFBSSxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQztRQUM1QyxJQUFJLENBQUMsT0FBTyxHQUFHLENBQUMsSUFBSSxDQUFDLEtBQUssSUFBSSxDQUFDLENBQUMsSUFBSSxDQUFDLE1BQU0sQ0FBQztRQUM1QyxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsQ0FBQyxJQUFJLENBQUMsS0FBSyxDQUFDO1FBRTNCLElBQUksQ0FBQyxRQUFRLEVBQUUsQ0FBQyxVQUFVLEVBQUUsQ0FBQztRQUM3QixJQUFJLENBQUMsWUFBWSxFQUFFLENBQUM7SUFDdEIsQ0FBQzs7Ozs7SUFFTyxjQUFjO1FBQ3BCLElBQUksQ0FBQyxJQUFJLENBQUMsT0FBTyxFQUFFO1lBQ2pCLE9BQU87U0FDUjs7Y0FFSyxhQUFhLEdBQUcsSUFBSSxDQUFDLE1BQU0sQ0FBQyxhQUFhLENBQUMsV0FBVzs7Y0FDckQsV0FBVyxHQUFHLElBQUksQ0FBQyxFQUFFLENBQUMscUJBQXFCLEVBQUUsQ0FBQyxLQUFLOztjQUNuRCxLQUFLLEdBQUcsV0FBVyxHQUFHLENBQUMsR0FBRyxhQUFhLENBQUMsQ0FBQyxDQUFDLENBQUMsV0FBVyxHQUFHLENBQUMsQ0FBQyxHQUFHLGFBQWEsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUNyRixJQUFJLENBQUMsVUFBVSxHQUFHO1lBQ2hCLFNBQVMsRUFBRSxTQUFTLEtBQUssb0JBQW9CO1NBQzlDLENBQUM7UUFDRixJQUFJLE9BQU8sSUFBSSxDQUFDLE1BQU0sS0FBSyxRQUFRLEVBQUU7WUFDbkMsTUFBTSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsVUFBVSxFQUFFO2dCQUM3QixVQUFVLEVBQUUsR0FBRyxJQUFJLENBQUMsTUFBTSxJQUFJO2FBQy9CLENBQUMsQ0FBQztTQUNKO1FBQ0QsSUFBSSxDQUFDLEVBQUUsQ0FBQyxhQUFhLEVBQUUsQ0FBQztJQUMxQixDQUFDOzs7Ozs7O0lBRU8sVUFBVTtRQUNoQix3RUFBd0U7UUFDeEUsVUFBVTs7O1FBQUMsR0FBRyxFQUFFO1lBQ2QsbUJBQUEsSUFBSSxFQUFBLENBQUMsY0FBYyxFQUFFLENBQUM7UUFDeEIsQ0FBQyxFQUFDLENBQUM7UUFDSCxPQUFPLG1CQUFBLElBQUksRUFBQSxDQUFDO0lBQ2QsQ0FBQzs7Ozs7SUFFTyxZQUFZO1FBQ2xCLElBQUksT0FBTyxJQUFJLENBQUMsTUFBTSxLQUFLLFFBQVEsRUFBRTtZQUNuQyxPQUFPO1NBQ1I7UUFDRCxJQUFJLENBQUMsUUFBUSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsRUFBRSxFQUFFLE9BQU8sRUFBRSxHQUFHLElBQUksQ0FBQyxNQUFNLElBQUksQ0FBQyxDQUFDO1FBQzdELElBQUksQ0FBQyxRQUFRLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxFQUFFLEVBQUUsUUFBUSxFQUFFLEdBQUcsSUFBSSxDQUFDLE1BQU0sSUFBSSxDQUFDLENBQUM7UUFDOUQsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSxhQUFhLEVBQUUsR0FBRyxJQUFJLENBQUMsTUFBTSxJQUFJLENBQUMsQ0FBQztRQUNuRSxJQUFJLElBQUksQ0FBQyxPQUFPLEVBQUU7WUFDaEIsSUFBSSxDQUFDLFFBQVEsQ0FBQyxRQUFRLENBQUMsSUFBSSxDQUFDLEVBQUUsRUFBRSxXQUFXLEVBQUUsR0FBRyxJQUFJLENBQUMsTUFBTSxHQUFHLENBQUMsSUFBSSxDQUFDLENBQUM7U0FDdEU7SUFDSCxDQUFDOzs7WUE3R0YsU0FBUyxTQUFDO2dCQUNULFFBQVEsRUFBRSxXQUFXO2dCQUNyQix3VEFBeUM7Z0JBQ3pDLFNBQVMsRUFBRSxDQUFDLHdCQUF3QixDQUFDO2dCQUNyQyxtQkFBbUIsRUFBRSxLQUFLO2dCQUMxQixlQUFlLEVBQUUsdUJBQXVCLENBQUMsTUFBTTtnQkFDL0MsYUFBYSxFQUFFLGlCQUFpQixDQUFDLElBQUk7YUFDdEM7Ozs7WUExQkMsVUFBVTtZQUZWLGlCQUFpQjtZQVdWLHdCQUF3QjtZQU4vQixTQUFTOzs7c0JBeUJSLEtBQUs7cUJBQ0wsS0FBSztxQkFDTCxLQUFLO29CQUNMLEtBQUs7cUJBQ0wsS0FBSztxQkFRTCxTQUFTLFNBQUMsUUFBUTs7OztJQVpuQixvQ0FBMkM7O0lBQzNDLG1DQUEwQzs7SUFDMUMsbUNBQXdCOztJQUN4QixrQ0FBdUI7O0lBQ3ZCLG1DQUF3Qjs7SUFFeEIsdUNBQWtCOztJQUNsQixvQ0FBeUI7O0lBQ3pCLG1DQUF1Qjs7SUFDdkIsb0NBQXlCOztJQUN6Qix1Q0FBZTs7SUFFZixtQ0FBd0M7Ozs7O0lBRXhDLCtCQUF3RDs7Ozs7SUFDeEQsc0NBQWlDOzs7OztJQUNqQyxvQ0FBMEQ7Ozs7O0lBR3hELHVDQUE4Qjs7Ozs7SUFDOUIsK0JBQTZCOzs7OztJQUM3QixtREFBd0Q7Ozs7O0lBQ3hELHFDQUEyQiIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBFbGVtZW50UmVmLFxuICBJbnB1dCxcbiAgT25DaGFuZ2VzLFxuICBSZW5kZXJlcjIsXG4gIFNpbXBsZUNoYW5nZXMsXG4gIFZpZXdDaGlsZCxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IE56VXBkYXRlSG9zdENsYXNzU2VydmljZSB9IGZyb20gJy4uL2NvcmUvc2VydmljZXMvdXBkYXRlLWhvc3QtY2xhc3Muc2VydmljZSc7XG5pbXBvcnQgeyBOelNpemVMRFNUeXBlLCBOelNpemVNYXAgfSBmcm9tICcuLi9jb3JlL3R5cGVzL3NpemUnO1xuXG5leHBvcnQgdHlwZSBOekF2YXRhclNoYXBlID0gJ3NxdWFyZScgfCAnY2lyY2xlJztcbmV4cG9ydCB0eXBlIE56QXZhdGFyU2l6ZSA9IE56U2l6ZUxEU1R5cGUgfCBudW1iZXI7XG5cbmV4cG9ydCBpbnRlcmZhY2UgTnpBdmF0YXJTaXplTWFwIHtcbiAgW3NpemU6IHN0cmluZ106IHN0cmluZztcbn1cblxuQENvbXBvbmVudCh7XG4gIHNlbGVjdG9yOiAnbnotYXZhdGFyJyxcbiAgdGVtcGxhdGVVcmw6ICcuL256LWF2YXRhci5jb21wb25lbnQuaHRtbCcsXG4gIHByb3ZpZGVyczogW056VXBkYXRlSG9zdENsYXNzU2VydmljZV0sXG4gIHByZXNlcnZlV2hpdGVzcGFjZXM6IGZhbHNlLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgZW5jYXBzdWxhdGlvbjogVmlld0VuY2Fwc3VsYXRpb24uTm9uZVxufSlcbmV4cG9ydCBjbGFzcyBOekF2YXRhckNvbXBvbmVudCBpbXBsZW1lbnRzIE9uQ2hhbmdlcyB7XG4gIEBJbnB1dCgpIG56U2hhcGU6IE56QXZhdGFyU2hhcGUgPSAnY2lyY2xlJztcbiAgQElucHV0KCkgbnpTaXplOiBOekF2YXRhclNpemUgPSAnZGVmYXVsdCc7XG4gIEBJbnB1dCgpIG56VGV4dDogc3RyaW5nO1xuICBASW5wdXQoKSBuelNyYzogc3RyaW5nO1xuICBASW5wdXQoKSBuekljb246IHN0cmluZztcblxuICBvbGRBUElJY29uID0gdHJ1ZTsgLy8gTWFrZSB0aGUgdXNlciBkZWZpbmVkIGljb24gY29tcGF0aWJsZSB0byBvbGQgQVBJLiBTaG91bGQgYmUgcmVtb3ZlZCBpbiAyLjAuXG4gIGhhc1RleHQ6IGJvb2xlYW4gPSBmYWxzZTtcbiAgaGFzU3JjOiBib29sZWFuID0gdHJ1ZTtcbiAgaGFzSWNvbjogYm9vbGVhbiA9IGZhbHNlO1xuICB0ZXh0U3R5bGVzOiB7fTtcblxuICBAVmlld0NoaWxkKCd0ZXh0RWwnKSB0ZXh0RWw6IEVsZW1lbnRSZWY7XG5cbiAgcHJpdmF0ZSBlbDogSFRNTEVsZW1lbnQgPSB0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudDtcbiAgcHJpdmF0ZSBwcmVmaXhDbHMgPSAnYW50LWF2YXRhcic7XG4gIHByaXZhdGUgc2l6ZU1hcDogTnpTaXplTWFwID0geyBsYXJnZTogJ2xnJywgc21hbGw6ICdzbScgfTtcblxuICBjb25zdHJ1Y3RvcihcbiAgICBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgcHJpdmF0ZSBjZDogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgcHJpdmF0ZSB1cGRhdGVIb3N0Q2xhc3NTZXJ2aWNlOiBOelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UsXG4gICAgcHJpdmF0ZSByZW5kZXJlcjogUmVuZGVyZXIyXG4gICkge31cblxuICBzZXRDbGFzcygpOiB0aGlzIHtcbiAgICBjb25zdCBjbGFzc01hcCA9IHtcbiAgICAgIFt0aGlzLnByZWZpeENsc106IHRydWUsXG4gICAgICBbYCR7dGhpcy5wcmVmaXhDbHN9LSR7dGhpcy5zaXplTWFwW3RoaXMubnpTaXplXX1gXTogdGhpcy5zaXplTWFwW3RoaXMubnpTaXplXSxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30tJHt0aGlzLm56U2hhcGV9YF06IHRoaXMubnpTaGFwZSxcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30taWNvbmBdOiB0aGlzLm56SWNvbixcbiAgICAgIFtgJHt0aGlzLnByZWZpeENsc30taW1hZ2VgXTogdGhpcy5oYXNTcmMgLy8gZG93bmdyYWRlIGFmdGVyIGltYWdlIGVycm9yXG4gICAgfTtcbiAgICB0aGlzLnVwZGF0ZUhvc3RDbGFzc1NlcnZpY2UudXBkYXRlSG9zdENsYXNzKHRoaXMuZWwsIGNsYXNzTWFwKTtcbiAgICB0aGlzLmNkLmRldGVjdENoYW5nZXMoKTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxuXG4gIGltZ0Vycm9yKCk6IHZvaWQge1xuICAgIHRoaXMuaGFzU3JjID0gZmFsc2U7XG4gICAgdGhpcy5oYXNJY29uID0gZmFsc2U7XG4gICAgdGhpcy5oYXNUZXh0ID0gZmFsc2U7XG4gICAgaWYgKHRoaXMubnpJY29uKSB7XG4gICAgICB0aGlzLmhhc0ljb24gPSB0cnVlO1xuICAgIH0gZWxzZSBpZiAodGhpcy5uelRleHQpIHtcbiAgICAgIHRoaXMuaGFzVGV4dCA9IHRydWU7XG4gICAgfVxuICAgIHRoaXMuc2V0Q2xhc3MoKS5ub3RpZnlDYWxjKCk7XG4gICAgdGhpcy5zZXRTaXplU3R5bGUoKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5oYXNPd25Qcm9wZXJ0eSgnbnpJY29uJykgJiYgY2hhbmdlcy5uekljb24uY3VycmVudFZhbHVlKSB7XG4gICAgICB0aGlzLm9sZEFQSUljb24gPSBjaGFuZ2VzLm56SWNvbi5jdXJyZW50VmFsdWUuaW5kZXhPZignYW50aWNvbicpID4gLTE7XG4gICAgfVxuICAgIHRoaXMuaGFzVGV4dCA9ICF0aGlzLm56U3JjICYmICEhdGhpcy5uelRleHQ7XG4gICAgdGhpcy5oYXNJY29uID0gIXRoaXMubnpTcmMgJiYgISF0aGlzLm56SWNvbjtcbiAgICB0aGlzLmhhc1NyYyA9ICEhdGhpcy5uelNyYztcblxuICAgIHRoaXMuc2V0Q2xhc3MoKS5ub3RpZnlDYWxjKCk7XG4gICAgdGhpcy5zZXRTaXplU3R5bGUoKTtcbiAgfVxuXG4gIHByaXZhdGUgY2FsY1N0cmluZ1NpemUoKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLmhhc1RleHQpIHtcbiAgICAgIHJldHVybjtcbiAgICB9XG5cbiAgICBjb25zdCBjaGlsZHJlbldpZHRoID0gdGhpcy50ZXh0RWwubmF0aXZlRWxlbWVudC5vZmZzZXRXaWR0aDtcbiAgICBjb25zdCBhdmF0YXJXaWR0aCA9IHRoaXMuZWwuZ2V0Qm91bmRpbmdDbGllbnRSZWN0KCkud2lkdGg7XG4gICAgY29uc3Qgc2NhbGUgPSBhdmF0YXJXaWR0aCAtIDggPCBjaGlsZHJlbldpZHRoID8gKGF2YXRhcldpZHRoIC0gOCkgLyBjaGlsZHJlbldpZHRoIDogMTtcbiAgICB0aGlzLnRleHRTdHlsZXMgPSB7XG4gICAgICB0cmFuc2Zvcm06IGBzY2FsZSgke3NjYWxlfSkgdHJhbnNsYXRlWCgtNTAlKWBcbiAgICB9O1xuICAgIGlmICh0eXBlb2YgdGhpcy5uelNpemUgPT09ICdudW1iZXInKSB7XG4gICAgICBPYmplY3QuYXNzaWduKHRoaXMudGV4dFN0eWxlcywge1xuICAgICAgICBsaW5lSGVpZ2h0OiBgJHt0aGlzLm56U2l6ZX1weGBcbiAgICAgIH0pO1xuICAgIH1cbiAgICB0aGlzLmNkLmRldGVjdENoYW5nZXMoKTtcbiAgfVxuXG4gIHByaXZhdGUgbm90aWZ5Q2FsYygpOiB0aGlzIHtcbiAgICAvLyBJZiB1c2UgbmdBZnRlclZpZXdDaGVja2VkLCBhbHdheXMgZGVtYW5kcyBtb3JlIGNvbXB1dGF0aW9ucywgc28uLi4uLi5cbiAgICBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgIHRoaXMuY2FsY1N0cmluZ1NpemUoKTtcbiAgICB9KTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxuXG4gIHByaXZhdGUgc2V0U2l6ZVN0eWxlKCk6IHZvaWQge1xuICAgIGlmICh0eXBlb2YgdGhpcy5uelNpemUgPT09ICdzdHJpbmcnKSB7XG4gICAgICByZXR1cm47XG4gICAgfVxuICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ3dpZHRoJywgYCR7dGhpcy5uelNpemV9cHhgKTtcbiAgICB0aGlzLnJlbmRlcmVyLnNldFN0eWxlKHRoaXMuZWwsICdoZWlnaHQnLCBgJHt0aGlzLm56U2l6ZX1weGApO1xuICAgIHRoaXMucmVuZGVyZXIuc2V0U3R5bGUodGhpcy5lbCwgJ2xpbmUtaGVpZ2h0JywgYCR7dGhpcy5uelNpemV9cHhgKTtcbiAgICBpZiAodGhpcy5oYXNJY29uKSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLnNldFN0eWxlKHRoaXMuZWwsICdmb250LXNpemUnLCBgJHt0aGlzLm56U2l6ZSAvIDJ9cHhgKTtcbiAgICB9XG4gIH1cbn1cbiJdfQ==