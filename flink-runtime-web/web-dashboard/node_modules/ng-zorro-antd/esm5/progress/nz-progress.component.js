/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ChangeDetectionStrategy, Component, Input, ViewEncapsulation } from '@angular/core';
import { InputNumber } from '../core/util';
import { isNotNil } from '../core/util/check';
var NzProgressComponent = /** @class */ (function () {
    function NzProgressComponent() {
        this.nzShowInfo = true;
        this.nzWidth = 132;
        this.nzType = 'line';
        this.nzStrokeLinecap = 'round';
        this.statusColorMap = {
            normal: '#108ee9',
            exception: '#ff5500',
            success: '#87d068'
        };
        this.cachedStatus = 'normal';
        this.inferredStatus = 'normal';
        this.inferredStrokeWidth = 8;
    }
    Object.defineProperty(NzProgressComponent.prototype, "formatter", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzFormat || ((/**
             * @param {?} p
             * @return {?}
             */
            function (p) { return p + "%"; }));
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzProgressComponent.prototype, "status", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzStatus || this.inferredStatus;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzProgressComponent.prototype, "strokeWidth", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzStrokeWidth || this.inferredStrokeWidth;
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(NzProgressComponent.prototype, "isCircleStyle", {
        get: /**
         * @return {?}
         */
        function () {
            return this.nzType === 'circle' || this.nzType === 'dashboard';
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @return {?}
     */
    NzProgressComponent.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        this.updatePathStyles();
        this.updateIcon();
    };
    /**
     * @param {?} changes
     * @return {?}
     */
    NzProgressComponent.prototype.ngOnChanges = /**
     * @param {?} changes
     * @return {?}
     */
    function (changes) {
        var nzGapPosition = changes.nzGapPosition, nzStrokeLinecap = changes.nzStrokeLinecap, nzGapDegree = changes.nzGapDegree, nzType = changes.nzType, nzSize = changes.nzSize, nzStatus = changes.nzStatus, nzPercent = changes.nzPercent, nzSuccessPercent = changes.nzSuccessPercent;
        if (nzGapPosition || nzStrokeLinecap || nzGapDegree || nzType || nzPercent) {
            this.updatePathStyles();
        }
        if (nzSize) {
            if (this.nzSize === 'small') {
                this.inferredStrokeWidth = 6;
            }
        }
        if (nzStatus) {
            this.cachedStatus = this.nzStatus || this.cachedStatus;
            this.updateIcon();
        }
        if (nzPercent || nzSuccessPercent) {
            /** @type {?} */
            var fillAll = parseInt(this.nzPercent.toString(), 10) >= 100;
            if (fillAll) {
                if ((isNotNil(this.nzSuccessPercent) && (/** @type {?} */ (this.nzSuccessPercent)) >= 100) || this.nzSuccessPercent === undefined) {
                    this.inferredStatus = 'success';
                }
            }
            else {
                this.inferredStatus = this.cachedStatus;
            }
            this.updateIcon();
        }
        if (nzType) {
            if (this.nzType !== 'line') {
                this.inferredStrokeWidth = 6;
            }
            if (this.nzType === 'dashboard') {
                this.inferredGapPosition = 'bottom';
                this.inferredGapDegree = 75;
            }
            if (this.nzType === 'circle') {
                this.inferredGapDegree = 0;
            }
        }
    };
    /**
     * @return {?}
     */
    NzProgressComponent.prototype.updatePathStyles = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var radius = 50 - this.strokeWidth / 2;
        /** @type {?} */
        var gapPosition = this.nzGapPosition || this.inferredGapPosition;
        /** @type {?} */
        var beginPositionX = 0;
        /** @type {?} */
        var beginPositionY = -radius;
        /** @type {?} */
        var endPositionX = 0;
        /** @type {?} */
        var endPositionY = radius * -2;
        switch (gapPosition) {
            case 'left':
                beginPositionX = -radius;
                beginPositionY = 0;
                endPositionX = radius * 2;
                endPositionY = 0;
                break;
            case 'right':
                beginPositionX = radius;
                beginPositionY = 0;
                endPositionX = radius * -2;
                endPositionY = 0;
                break;
            case 'bottom':
                beginPositionY = radius;
                endPositionY = radius * 2;
                break;
            default:
        }
        this.pathString = "M 50,50 m " + beginPositionX + "," + beginPositionY + "\n     a " + radius + "," + radius + " 0 1 1 " + endPositionX + "," + -endPositionY + "\n     a " + radius + "," + radius + " 0 1 1 " + -endPositionX + "," + endPositionY;
        /** @type {?} */
        var len = Math.PI * 2 * radius;
        /** @type {?} */
        var gapDegree = this.nzGapDegree || this.inferredGapDegree;
        this.trailPathStyle = {
            strokeDasharray: len - gapDegree + "px " + len + "px",
            strokeDashoffset: "-" + gapDegree / 2 + "px",
            transition: 'stroke-dashoffset .3s ease 0s, stroke-dasharray .3s ease 0s, stroke .3s'
        };
        this.strokePathStyle = {
            strokeDasharray: (this.nzPercent / 100) * (len - gapDegree) + "px " + len + "px",
            strokeDashoffset: "-" + gapDegree / 2 + "px",
            transition: 'stroke-dashoffset .3s ease 0s, stroke-dasharray .3s ease 0s, stroke .3s, stroke-width .06s ease .3s' // eslint-disable-line
        };
    };
    /**
     * @return {?}
     */
    NzProgressComponent.prototype.updateIcon = /**
     * @return {?}
     */
    function () {
        /** @type {?} */
        var isCircle = this.nzType === 'circle' || this.nzType === 'dashboard';
        /** @type {?} */
        var ret = this.status === 'success' ? 'check' : this.status === 'exception' ? 'close' : '';
        this.icon = ret ? ret + (isCircle ? '-o' : '-circle-fill') : '';
    };
    NzProgressComponent.decorators = [
        { type: Component, args: [{
                    changeDetection: ChangeDetectionStrategy.OnPush,
                    encapsulation: ViewEncapsulation.None,
                    selector: 'nz-progress',
                    preserveWhitespaces: false,
                    template: "<ng-template #progressInfoTemplate>\n  <span class=\"ant-progress-text\" *ngIf=\"nzShowInfo\">\n    <ng-container *ngIf=\"status === 'exception' || (status === 'success' && !nzFormat); else formatTemplate\">\n      <i nz-icon [nzType]=\"icon\"></i>\n    </ng-container>\n    <ng-template #formatTemplate>\n      {{ formatter(nzPercent) }}\n    </ng-template>\n  </span>\n</ng-template>\n<div\n  [ngClass]=\"'ant-progress ant-progress-status-' + status\"\n  [class.ant-progress-line]=\"nzType == 'line'\"\n  [class.ant-progress-small]=\"nzSize == 'small'\"\n  [class.ant-progress-show-info]=\"nzShowInfo\"\n  [class.ant-progress-circle]=\"isCircleStyle\"\n>\n  <!-- Line progress -->\n  <div *ngIf=\"nzType === 'line'\">\n    <div class=\"ant-progress-outer\">\n      <div class=\"ant-progress-inner\">\n        <div\n          class=\"ant-progress-bg\"\n          [style.width.%]=\"nzPercent\"\n          [style.border-radius]=\"nzStrokeLinecap === 'round' ? '100px' : '0'\"\n          [style.background]=\"nzStrokeColor\"\n          [style.height.px]=\"strokeWidth\"\n        ></div>\n        <div\n          class=\"ant-progress-success-bg\"\n          [style.width.%]=\"nzSuccessPercent\"\n          [style.border-radius]=\"nzStrokeLinecap === 'round' ? '100px' : '0'\"\n          [style.height.px]=\"strokeWidth\"\n        ></div>\n      </div>\n    </div>\n    <ng-template [ngTemplateOutlet]=\"progressInfoTemplate\"></ng-template>\n  </div>\n  <!-- Circle/Dashboard progress -->\n  <div\n    [style.width.px]=\"this.nzWidth\"\n    [style.height.px]=\"this.nzWidth\"\n    [style.fontSize.px]=\"this.nzWidth * 0.15 + 6\"\n    class=\"ant-progress-inner\"\n    *ngIf=\"isCircleStyle\"\n  >\n    <svg class=\"ant-progress-circle \" viewBox=\"0 0 100 100\">\n      <path\n        class=\"ant-progress-circle-trail\"\n        stroke=\"#f3f3f3\"\n        fill-opacity=\"0\"\n        [attr.stroke-width]=\"strokeWidth\"\n        [ngStyle]=\"trailPathStyle\"\n        [attr.d]=\"pathString\"\n      ></path>\n      <path\n        class=\"ant-progress-circle-path\"\n        [attr.d]=\"pathString\"\n        [attr.stroke-linecap]=\"nzStrokeLinecap\"\n        fill-opacity=\"0\"\n        [attr.stroke]=\"nzStrokeColor || statusColorMap[status]\"\n        [attr.stroke-width]=\"nzPercent ? strokeWidth : 0\"\n        [ngStyle]=\"strokePathStyle\"\n      ></path>\n    </svg>\n    <ng-template [ngTemplateOutlet]=\"progressInfoTemplate\"></ng-template>\n  </div>\n</div>\n"
                }] }
    ];
    NzProgressComponent.propDecorators = {
        nzShowInfo: [{ type: Input }],
        nzWidth: [{ type: Input }],
        nzStrokeColor: [{ type: Input }],
        nzSize: [{ type: Input }],
        nzFormat: [{ type: Input }],
        nzSuccessPercent: [{ type: Input }],
        nzPercent: [{ type: Input }],
        nzStrokeWidth: [{ type: Input }],
        nzGapDegree: [{ type: Input }],
        nzStatus: [{ type: Input }],
        nzType: [{ type: Input }],
        nzGapPosition: [{ type: Input }],
        nzStrokeLinecap: [{ type: Input }]
    };
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Number)
    ], NzProgressComponent.prototype, "nzSuccessPercent", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Number)
    ], NzProgressComponent.prototype, "nzPercent", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Number)
    ], NzProgressComponent.prototype, "nzStrokeWidth", void 0);
    tslib_1.__decorate([
        InputNumber(),
        tslib_1.__metadata("design:type", Number)
    ], NzProgressComponent.prototype, "nzGapDegree", void 0);
    return NzProgressComponent;
}());
export { NzProgressComponent };
if (false) {
    /** @type {?} */
    NzProgressComponent.prototype.nzShowInfo;
    /** @type {?} */
    NzProgressComponent.prototype.nzWidth;
    /** @type {?} */
    NzProgressComponent.prototype.nzStrokeColor;
    /** @type {?} */
    NzProgressComponent.prototype.nzSize;
    /** @type {?} */
    NzProgressComponent.prototype.nzFormat;
    /** @type {?} */
    NzProgressComponent.prototype.nzSuccessPercent;
    /** @type {?} */
    NzProgressComponent.prototype.nzPercent;
    /** @type {?} */
    NzProgressComponent.prototype.nzStrokeWidth;
    /** @type {?} */
    NzProgressComponent.prototype.nzGapDegree;
    /** @type {?} */
    NzProgressComponent.prototype.nzStatus;
    /** @type {?} */
    NzProgressComponent.prototype.nzType;
    /** @type {?} */
    NzProgressComponent.prototype.nzGapPosition;
    /** @type {?} */
    NzProgressComponent.prototype.nzStrokeLinecap;
    /** @type {?} */
    NzProgressComponent.prototype.trailPathStyle;
    /** @type {?} */
    NzProgressComponent.prototype.strokePathStyle;
    /** @type {?} */
    NzProgressComponent.prototype.pathString;
    /** @type {?} */
    NzProgressComponent.prototype.icon;
    /** @type {?} */
    NzProgressComponent.prototype.statusColorMap;
    /**
     * @type {?}
     * @private
     */
    NzProgressComponent.prototype.cachedStatus;
    /**
     * @type {?}
     * @private
     */
    NzProgressComponent.prototype.inferredStatus;
    /**
     * @type {?}
     * @private
     */
    NzProgressComponent.prototype.inferredStrokeWidth;
    /**
     * @type {?}
     * @private
     */
    NzProgressComponent.prototype.inferredGapPosition;
    /**
     * @type {?}
     * @private
     */
    NzProgressComponent.prototype.inferredGapDegree;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcHJvZ3Jlc3MuY29tcG9uZW50LmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInByb2dyZXNzL256LXByb2dyZXNzLmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFDTCx1QkFBdUIsRUFDdkIsU0FBUyxFQUNULEtBQUssRUFJTCxpQkFBaUIsRUFDbEIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLFdBQVcsRUFBRSxNQUFNLGNBQWMsQ0FBQztBQUMzQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sb0JBQW9CLENBQUM7QUFPOUM7SUFBQTtRQVFXLGVBQVUsR0FBRyxJQUFJLENBQUM7UUFDbEIsWUFBTyxHQUFHLEdBQUcsQ0FBQztRQVNkLFdBQU0sR0FBdUIsTUFBTSxDQUFDO1FBRXBDLG9CQUFlLEdBQWdDLE9BQU8sQ0FBQztRQU1oRSxtQkFBYyxHQUE4QjtZQUMxQyxNQUFNLEVBQUUsU0FBUztZQUNqQixTQUFTLEVBQUUsU0FBUztZQUNwQixPQUFPLEVBQUUsU0FBUztTQUNuQixDQUFDO1FBRU0saUJBQVksR0FBeUIsUUFBUSxDQUFDO1FBQzlDLG1CQUFjLEdBQXlCLFFBQVEsQ0FBQztRQUNoRCx3QkFBbUIsR0FBVyxDQUFDLENBQUM7SUFtSTFDLENBQUM7SUEvSEMsc0JBQUksMENBQVM7Ozs7UUFBYjtZQUNFLE9BQU8sSUFBSSxDQUFDLFFBQVEsSUFBSTs7OztZQUFDLFVBQUMsQ0FBUyxJQUFhLE9BQUcsQ0FBQyxNQUFHLEVBQVAsQ0FBTyxFQUFDLENBQUM7UUFDM0QsQ0FBQzs7O09BQUE7SUFFRCxzQkFBSSx1Q0FBTTs7OztRQUFWO1lBQ0UsT0FBTyxJQUFJLENBQUMsUUFBUSxJQUFJLElBQUksQ0FBQyxjQUFjLENBQUM7UUFDOUMsQ0FBQzs7O09BQUE7SUFFRCxzQkFBSSw0Q0FBVzs7OztRQUFmO1lBQ0UsT0FBTyxJQUFJLENBQUMsYUFBYSxJQUFJ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O0lBQ3ZCLDRDQUErQjs7SUFDL0IscUNBQXdCOztJQUN4Qix1Q0FBZ0Q7O0lBQ2hELCtDQUFrRDs7SUFDbEQsd0NBQTBDOztJQUMxQyw0Q0FBOEM7O0lBQzlDLDBDQUE0Qzs7SUFDNUMsdUNBQXdDOztJQUN4QyxxQ0FBNkM7O0lBQzdDLDRDQUFtRDs7SUFDbkQsOENBQWdFOztJQUVoRSw2Q0FBMEM7O0lBQzFDLDhDQUEyQzs7SUFDM0MseUNBQW1COztJQUNuQixtQ0FBYTs7SUFDYiw2Q0FJRTs7Ozs7SUFFRiwyQ0FBc0Q7Ozs7O0lBQ3RELDZDQUF3RDs7Ozs7SUFDeEQsa0RBQXdDOzs7OztJQUN4QyxrREFBb0M7Ozs7O0lBQ3BDLGdEQUFrQyIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7XG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDb21wb25lbnQsXG4gIElucHV0LFxuICBPbkNoYW5nZXMsXG4gIE9uSW5pdCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVmlld0VuY2Fwc3VsYXRpb25cbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IElucHV0TnVtYmVyIH0gZnJvbSAnLi4vY29yZS91dGlsJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcblxuZXhwb3J0IHR5cGUgTnpQcm9ncmVzc0dhcFBvc2l0aW9uVHlwZSA9ICd0b3AnIHwgJ2JvdHRvbScgfCAnbGVmdCcgfCAncmlnaHQnO1xuZXhwb3J0IHR5cGUgTnpQcm9ncmVzc1N0YXR1c1R5cGUgPSAnc3VjY2VzcycgfCAnZXhjZXB0aW9uJyB8ICdhY3RpdmUnIHwgJ25vcm1hbCc7XG5leHBvcnQgdHlwZSBOelByb2dyZXNzVHlwZVR5cGUgPSAnbGluZScgfCAnY2lyY2xlJyB8ICdkYXNoYm9hcmQnO1xuZXhwb3J0IHR5cGUgTnpQcm9ncmVzc1N0cm9rZUxpbmVjYXBUeXBlID0gJ3JvdW5kJyB8ICdzcXVhcmUnO1xuXG5AQ29tcG9uZW50KHtcbiAgY2hhbmdlRGV0ZWN0aW9uOiBDaGFuZ2VEZXRlY3Rpb25TdHJhdGVneS5PblB1c2gsXG4gIGVuY2Fwc3VsYXRpb246IFZpZXdFbmNhcHN1bGF0aW9uLk5vbmUsXG4gIHNlbGVjdG9yOiAnbnotcHJvZ3Jlc3MnLFxuICBwcmVzZXJ2ZVdoaXRlc3BhY2VzOiBmYWxzZSxcbiAgdGVtcGxhdGVVcmw6ICcuL256LXByb2dyZXNzLmNvbXBvbmVudC5odG1sJ1xufSlcbmV4cG9ydCBjbGFzcyBOelByb2dyZXNzQ29tcG9uZW50IGltcGxlbWVudHMgT25Jbml0LCBPbkNoYW5nZXMge1xuICBASW5wdXQoKSBuelNob3dJbmZvID0gdHJ1ZTtcbiAgQElucHV0KCkgbnpXaWR0aCA9IDEzMjtcbiAgQElucHV0KCkgbnpTdHJva2VDb2xvcjogc3RyaW5nO1xuICBASW5wdXQoKSBuelNpemU6IHN0cmluZztcbiAgQElucHV0KCkgbnpGb3JtYXQ/OiAocGVyY2VudDogbnVtYmVyKSA9PiBzdHJpbmc7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56U3VjY2Vzc1BlcmNlbnQ/OiBudW1iZXI7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56UGVyY2VudDogbnVtYmVyO1xuICBASW5wdXQoKSBASW5wdXROdW1iZXIoKSBuelN0cm9rZVdpZHRoOiBudW1iZXI7XG4gIEBJbnB1dCgpIEBJbnB1dE51bWJlcigpIG56R2FwRGVncmVlOiBudW1iZXI7XG4gIEBJbnB1dCgpIG56U3RhdHVzOiBOelByb2dyZXNzU3RhdHVzVHlwZTtcbiAgQElucHV0KCkgbnpUeXBlOiBOelByb2dyZXNzVHlwZVR5cGUgPSAnbGluZSc7XG4gIEBJbnB1dCgpIG56R2FwUG9zaXRpb24/OiBOelByb2dyZXNzR2FwUG9zaXRpb25UeXBlO1xuICBASW5wdXQoKSBuelN0cm9rZUxpbmVjYXA6IE56UHJvZ3Jlc3NTdHJva2VMaW5lY2FwVHlwZSA9ICdyb3VuZCc7XG5cbiAgdHJhaWxQYXRoU3R5bGU6IHsgW2tleTogc3RyaW5nXTogc3RyaW5nIH07XG4gIHN0cm9rZVBhdGhTdHlsZTogeyBba2V5OiBzdHJpbmddOiBzdHJpbmcgfTtcbiAgcGF0aFN0cmluZzogc3RyaW5nO1xuICBpY29uOiBzdHJpbmc7XG4gIHN0YXR1c0NvbG9yTWFwOiB7IFtrZXk6IHN0cmluZ106IHN0cmluZyB9ID0ge1xuICAgIG5vcm1hbDogJyMxMDhlZTknLFxuICAgIGV4Y2VwdGlvbjogJyNmZjU1MDAnLFxuICAgIHN1Y2Nlc3M6ICcjODdkMDY4J1xuICB9O1xuXG4gIHByaXZhdGUgY2FjaGVkU3RhdHVzOiBOelByb2dyZXNzU3RhdHVzVHlwZSA9ICdub3JtYWwnO1xuICBwcml2YXRlIGluZmVycmVkU3RhdHVzOiBOelByb2dyZXNzU3RhdHVzVHlwZSA9ICdub3JtYWwnO1xuICBwcml2YXRlIGluZmVycmVkU3Ryb2tlV2lkdGg6IG51bWJlciA9IDg7XG4gIHByaXZhdGUgaW5mZXJyZWRHYXBQb3NpdGlvbjogc3RyaW5nO1xuICBwcml2YXRlIGluZmVycmVkR2FwRGVncmVlOiBudW1iZXI7XG5cbiAgZ2V0IGZvcm1hdHRlcigpOiAocGVyY2VudDogbnVtYmVyKSA9PiBzdHJpbmcge1xuICAgIHJldHVybiB0aGlzLm56Rm9ybWF0IHx8ICgocDogbnVtYmVyKTogc3RyaW5nID0+IGAke3B9JWApO1xuICB9XG5cbiAgZ2V0IHN0YXR1cygpOiBOelByb2dyZXNzU3RhdHVzVHlwZSB7XG4gICAgcmV0dXJuIHRoaXMubnpTdGF0dXMgfHwgdGhpcy5pbmZlcnJlZFN0YXR1cztcbiAgfVxuXG4gIGdldCBzdHJva2VXaWR0aCgpOiBudW1iZXIge1xuICAgIHJldHVybiB0aGlzLm56U3Ryb2tlV2lkdGggfHwgdGhpcy5pbmZlcnJlZFN0cm9rZVdpZHRoO1xuICB9XG5cbiAgZ2V0IGlzQ2lyY2xlU3R5bGUoKTogYm9vbGVhbiB7XG4gICAgcmV0dXJuIHRoaXMubnpUeXBlID09PSAnY2lyY2xlJyB8fCB0aGlzLm56VHlwZSA9PT0gJ2Rhc2hib2FyZCc7XG4gIH1cblxuICBuZ09uSW5pdCgpOiB2b2lkIHtcbiAgICB0aGlzLnVwZGF0ZVBhdGhTdHlsZXMoKTtcbiAgICB0aGlzLnVwZGF0ZUljb24oKTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBjb25zdCB7XG4gICAgICBuekdhcFBvc2l0aW9uLFxuICAgICAgbnpTdHJva2VMaW5lY2FwLFxuICAgICAgbnpHYXBEZWdyZWUsXG4gICAgICBuelR5cGUsXG4gICAgICBuelNpemUsXG4gICAgICBuelN0YXR1cyxcbiAgICAgIG56UGVyY2VudCxcbiAgICAgIG56U3VjY2Vzc1BlcmNlbnRcbiAgICB9ID0gY2hhbmdlcztcblxuICAgIGlmIChuekdhcFBvc2l0aW9uIHx8IG56U3Ryb2tlTGluZWNhcCB8fCBuekdhcERlZ3JlZSB8fCBuelR5cGUgfHwgbnpQZXJjZW50KSB7XG4gICAgICB0aGlzLnVwZGF0ZVBhdGhTdHlsZXMoKTtcbiAgICB9XG5cbiAgICBpZiAobnpTaXplKSB7XG4gICAgICBpZiAodGhpcy5uelNpemUgPT09ICdzbWFsbCcpIHtcbiAgICAgICAgdGhpcy5pbmZlcnJlZFN0cm9rZVdpZHRoID0gNjtcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAobnpTdGF0dXMpIHtcbiAgICAgIHRoaXMuY2FjaGVkU3RhdHVzID0gdGhpcy5uelN0YXR1cyB8fCB0aGlzLmNhY2hlZFN0YXR1cztcbiAgICAgIHRoaXMudXBkYXRlSWNvbigpO1xuICAgIH1cblxuICAgIGlmIChuelBlcmNlbnQgfHwgbnpTdWNjZXNzUGVyY2VudCkge1xuICAgICAgY29uc3QgZmlsbEFsbCA9IHBhcnNlSW50KHRoaXMubnpQZXJjZW50LnRvU3RyaW5nKCksIDEwKSA+PSAxMDA7XG4gICAgICBpZiAoZmlsbEFsbCkge1xuICAgICAgICBpZiAoKGlzTm90TmlsKHRoaXMubnpTdWNjZXNzUGVyY2VudCkgJiYgdGhpcy5uelN1Y2Nlc3NQZXJjZW50ISA+PSAxMDApIHx8IHRoaXMubnpTdWNjZXNzUGVyY2VudCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgdGhpcy5pbmZlcnJlZFN0YXR1cyA9ICdzdWNjZXNzJztcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5pbmZlcnJlZFN0YXR1cyA9IHRoaXMuY2FjaGVkU3RhdHVzO1xuICAgICAgfVxuICAgICAgdGhpcy51cGRhdGVJY29uKCk7XG4gICAgfVxuXG4gICAgaWYgKG56VHlwZSkge1xuICAgICAgaWYgKHRoaXMubnpUeXBlICE9PSAnbGluZScpIHtcbiAgICAgICAgdGhpcy5pbmZlcnJlZFN0cm9rZVdpZHRoID0gNjtcbiAgICAgIH1cbiAgICAgIGlmICh0aGlzLm56VHlwZSA9PT0gJ2Rhc2hib2FyZCcpIHtcbiAgICAgICAgdGhpcy5pbmZlcnJlZEdhcFBvc2l0aW9uID0gJ2JvdHRvbSc7XG4gICAgICAgIHRoaXMuaW5mZXJyZWRHYXBEZWdyZWUgPSA3NTtcbiAgICAgIH1cbiAgICAgIGlmICh0aGlzLm56VHlwZSA9PT0gJ2NpcmNsZScpIHtcbiAgICAgICAgdGhpcy5pbmZlcnJlZEdhcERlZ3JlZSA9IDA7XG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgdXBkYXRlUGF0aFN0eWxlcygpOiB2b2lkIHtcbiAgICBjb25zdCByYWRpdXMgPSA1MCAtIHRoaXMuc3Ryb2tlV2lkdGggLyAyO1xuICAgIGNvbnN0IGdhcFBvc2l0aW9uID0gdGhpcy5uekdhcFBvc2l0aW9uIHx8IHRoaXMuaW5mZXJyZWRHYXBQb3NpdGlvbjtcbiAgICBsZXQgYmVnaW5Qb3NpdGlvblggPSAwO1xuICAgIGxldCBiZWdpblBvc2l0aW9uWSA9IC1yYWRpdXM7XG4gICAgbGV0IGVuZFBvc2l0aW9uWCA9IDA7XG4gICAgbGV0IGVuZFBvc2l0aW9uWSA9IHJhZGl1cyAqIC0yO1xuXG4gICAgc3dpdGNoIChnYXBQb3NpdGlvbikge1xuICAgICAgY2FzZSAnbGVmdCc6XG4gICAgICAgIGJlZ2luUG9zaXRpb25YID0gLXJhZGl1cztcbiAgICAgICAgYmVnaW5Qb3NpdGlvblkgPSAwO1xuICAgICAgICBlbmRQb3NpdGlvblggPSByYWRpdXMgKiAyO1xuICAgICAgICBlbmRQb3NpdGlvblkgPSAwO1xuICAgICAgICBicmVhaztcbiAgICAgIGNhc2UgJ3JpZ2h0JzpcbiAgICAgICAgYmVnaW5Qb3NpdGlvblggPSByYWRpdXM7XG4gICAgICAgIGJlZ2luUG9zaXRpb25ZID0gMDtcbiAgICAgICAgZW5kUG9zaXRpb25YID0gcmFkaXVzICogLTI7XG4gICAgICAgIGVuZFBvc2l0aW9uWSA9IDA7XG4gICAgICAgIGJyZWFrO1xuICAgICAgY2FzZSAnYm90dG9tJzpcbiAgICAgICAgYmVnaW5Qb3NpdGlvblkgPSByYWRpdXM7XG4gICAgICAgIGVuZFBvc2l0aW9uWSA9IHJhZGl1cyAqIDI7XG4gICAgICAgIGJyZWFrO1xuICAgICAgZGVmYXVsdDpcbiAgICB9XG5cbiAgICB0aGlzLnBhdGhTdHJpbmcgPSBgTSA1MCw1MCBtICR7YmVnaW5Qb3NpdGlvblh9LCR7YmVnaW5Qb3NpdGlvbll9XG4gICAgIGEgJHtyYWRpdXN9LCR7cmFkaXVzfSAwIDEgMSAke2VuZFBvc2l0aW9uWH0sJHstZW5kUG9zaXRpb25ZfVxuICAgICBhICR7cmFkaXVzfSwke3JhZGl1c30gMCAxIDEgJHstZW5kUG9zaXRpb25YfSwke2VuZFBvc2l0aW9uWX1gO1xuXG4gICAgY29uc3QgbGVuID0gTWF0aC5QSSAqIDIgKiByYWRpdXM7XG4gICAgY29uc3QgZ2FwRGVncmVlID0gdGhpcy5uekdhcERlZ3JlZSB8fCB0aGlzLmluZmVycmVkR2FwRGVncmVlO1xuXG4gICAgdGhpcy50cmFpbFBhdGhTdHlsZSA9IHtcbiAgICAgIHN0cm9rZURhc2hhcnJheTogYCR7bGVuIC0gZ2FwRGVncmVlfXB4ICR7bGVufXB4YCxcbiAgICAgIHN0cm9rZURhc2hvZmZzZXQ6IGAtJHtnYXBEZWdyZWUgLyAyfXB4YCxcbiAgICAgIHRyYW5zaXRpb246ICdzdHJva2UtZGFzaG9mZnNldCAuM3MgZWFzZSAwcywgc3Ryb2tlLWRhc2hhcnJheSAuM3MgZWFzZSAwcywgc3Ryb2tlIC4zcydcbiAgICB9O1xuICAgIHRoaXMuc3Ryb2tlUGF0aFN0eWxlID0ge1xuICAgICAgc3Ryb2tlRGFzaGFycmF5OiBgJHsodGhpcy5uelBlcmNlbnQgLyAxMDApICogKGxlbiAtIGdhcERlZ3JlZSl9cHggJHtsZW59cHhgLFxuICAgICAgc3Ryb2tlRGFzaG9mZnNldDogYC0ke2dhcERlZ3JlZSAvIDJ9cHhgLFxuICAgICAgdHJhbnNpdGlvbjogJ3N0cm9rZS1kYXNob2Zmc2V0IC4zcyBlYXNlIDBzLCBzdHJva2UtZGFzaGFycmF5IC4zcyBlYXNlIDBzLCBzdHJva2UgLjNzLCBzdHJva2Utd2lkdGggLjA2cyBlYXNlIC4zcycgLy8gZXNsaW50LWRpc2FibGUtbGluZVxuICAgIH07XG4gIH1cblxuICB1cGRhdGVJY29uKCk6IHZvaWQge1xuICAgIGNvbnN0IGlzQ2lyY2xlID0gdGhpcy5uelR5cGUgPT09ICdjaXJjbGUnIHx8IHRoaXMubnpUeXBlID09PSAnZGFzaGJvYXJkJztcbiAgICBjb25zdCByZXQgPSB0aGlzLnN0YXR1cyA9PT0gJ3N1Y2Nlc3MnID8gJ2NoZWNrJyA6IHRoaXMuc3RhdHVzID09PSAnZXhjZXB0aW9uJyA/ICdjbG9zZScgOiAnJztcblxuICAgIHRoaXMuaWNvbiA9IHJldCA/IHJldCArIChpc0NpcmNsZSA/ICctbycgOiAnLWNpcmNsZS1maWxsJykgOiAnJztcbiAgfVxufVxuIl19