/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { ComponentFactoryResolver, Directive, ElementRef, EventEmitter, Host, Input, Optional, Output, Renderer2, ViewContainerRef } from '@angular/core';
import { distinctUntilChanged } from 'rxjs/operators';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { InputBoolean } from '../core/util/convert';
import { NzTooltipDirective } from '../tooltip/nz-tooltip.directive';
import { NzPopconfirmComponent } from './nz-popconfirm.component';
var NzPopconfirmDirective = /** @class */ (function (_super) {
    tslib_1.__extends(NzPopconfirmDirective, _super);
    function NzPopconfirmDirective(elementRef, hostView, resolver, renderer, tooltip, noAnimation) {
        var _this = _super.call(this, elementRef, hostView, resolver, renderer, tooltip, noAnimation) || this;
        _this.noAnimation = noAnimation;
        _this.factory = _this.resolver.resolveComponentFactory(NzPopconfirmComponent);
        _this.needProxyProperties = [
            'nzTitle',
            'nzContent',
            'nzOverlayClassName',
            'nzOverlayStyle',
            'nzMouseEnterDelay',
            'nzMouseLeaveDelay',
            'nzVisible',
            'nzTrigger',
            'nzPlacement',
            'nzOkText',
            'nzOkType',
            'nzCancelText',
            'nzCondition',
            'nzIcon'
        ];
        _this.nzOnCancel = new EventEmitter();
        _this.nzOnConfirm = new EventEmitter();
        return _this;
    }
    /**
     * @return {?}
     */
    NzPopconfirmDirective.prototype.ngOnInit = /**
     * @return {?}
     */
    function () {
        var _this = this;
        if (!this.tooltip) {
            /** @type {?} */
            var tooltipComponent = this.hostView.createComponent(this.factory);
            this.tooltip = tooltipComponent.instance;
            this.tooltip.noAnimation = this.noAnimation;
            // Remove element when use directive https://github.com/NG-ZORRO/ng-zorro-antd/issues/1967
            this.renderer.removeChild(this.renderer.parentNode(this.elementRef.nativeElement), tooltipComponent.location.nativeElement);
            this.isDynamicTooltip = true;
            this.needProxyProperties.forEach((/**
             * @param {?} property
             * @return {?}
             */
            function (property) { return _this.updateCompValue(property, _this[property]); }));
            /** @type {?} */
            var visible_ = this.tooltip.nzVisibleChange.pipe(distinctUntilChanged()).subscribe((/**
             * @param {?} data
             * @return {?}
             */
            function (data) {
                _this.visible = data;
                _this.nzVisibleChange.emit(data);
            }));
            /** @type {?} */
            var cancel_ = ((/** @type {?} */ (this.tooltip))).nzOnCancel.subscribe((/**
             * @return {?}
             */
            function () {
                _this.nzOnCancel.emit();
            }));
            /** @type {?} */
            var confirm_ = ((/** @type {?} */ (this.tooltip))).nzOnConfirm.subscribe((/**
             * @return {?}
             */
            function () {
                _this.nzOnConfirm.emit();
            }));
            this.subs_.add(visible_);
            this.subs_.add(cancel_);
            this.subs_.add(confirm_);
        }
        this.tooltip.setOverlayOrigin(this);
    };
    NzPopconfirmDirective.decorators = [
        { type: Directive, args: [{
                    selector: '[nz-popconfirm]',
                    host: {
                        '[class.ant-popover-open]': 'isTooltipOpen'
                    }
                },] }
    ];
    /** @nocollapse */
    NzPopconfirmDirective.ctorParameters = function () { return [
        { type: ElementRef },
        { type: ViewContainerRef },
        { type: ComponentFactoryResolver },
        { type: Renderer2 },
        { type: NzPopconfirmComponent, decorators: [{ type: Optional }] },
        { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
    ]; };
    NzPopconfirmDirective.propDecorators = {
        nzOkText: [{ type: Input }],
        nzOkType: [{ type: Input }],
        nzCancelText: [{ type: Input }],
        nzIcon: [{ type: Input }],
        nzCondition: [{ type: Input }],
        nzOnCancel: [{ type: Output }],
        nzOnConfirm: [{ type: Output }]
    };
    tslib_1.__decorate([
        InputBoolean(),
        tslib_1.__metadata("design:type", Boolean)
    ], NzPopconfirmDirective.prototype, "nzCondition", void 0);
    return NzPopconfirmDirective;
}(NzTooltipDirective));
export { NzPopconfirmDirective };
if (false) {
    /** @type {?} */
    NzPopconfirmDirective.prototype.factory;
    /**
     * @type {?}
     * @protected
     */
    NzPopconfirmDirective.prototype.needProxyProperties;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzOkText;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzOkType;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzCancelText;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzIcon;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzCondition;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzOnCancel;
    /** @type {?} */
    NzPopconfirmDirective.prototype.nzOnConfirm;
    /** @type {?} */
    NzPopconfirmDirective.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wY29uZmlybS5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wY29uZmlybS9uei1wb3Bjb25maXJtLmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFFTCx3QkFBd0IsRUFDeEIsU0FBUyxFQUNULFVBQVUsRUFDVixZQUFZLEVBQ1osSUFBSSxFQUNKLEtBQUssRUFFTCxRQUFRLEVBQ1IsTUFBTSxFQUNOLFNBQVMsRUFFVCxnQkFBZ0IsRUFDakIsTUFBTSxlQUFlLENBQUM7QUFFdkIsT0FBTyxFQUFFLG9CQUFvQixFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFFdEQsT0FBTyxFQUFFLHNCQUFzQixFQUFFLE1BQU0sZ0RBQWdELENBQUM7QUFDeEYsT0FBTyxFQUFFLFlBQVksRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3BELE9BQU8sRUFBRSxrQkFBa0IsRUFBRSxNQUFNLGlDQUFpQyxDQUFDO0FBQ3JFLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLDJCQUEyQixDQUFDO0FBRWxFO0lBTTJDLGlEQUFrQjtJQTRCM0QsK0JBQ0UsVUFBc0IsRUFDdEIsUUFBMEIsRUFDMUIsUUFBa0MsRUFDbEMsUUFBbUIsRUFDUCxPQUE4QixFQUNmLFdBQW9DO1FBTmpFLFlBUUUsa0JBQU0sVUFBVSxFQUFFLFFBQVEsRUFBRSxRQUFRLEVBQUUsUUFBUSxFQUFFLE9BQU8sRUFBRSxXQUFXLENBQUMsU0FDdEU7UUFINEIsaUJBQVcsR0FBWCxXQUFXLENBQXlCO1FBakNqRSxhQUFPLEdBQTRDLEtBQUksQ0FBQyxRQUFRLENBQUMsdUJBQXVCLENBQUMscUJBQXFCLENBQUMsQ0FBQztRQUV0Ryx5QkFBbUIsR0FBRztZQUM5QixTQUFTO1lBQ1QsV0FBVztZQUNYLG9CQUFvQjtZQUNwQixnQkFBZ0I7WUFDaEIsbUJBQW1CO1lBQ25CLG1CQUFtQjtZQUNuQixXQUFXO1lB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R0FtRTVEO1NBbkVZLHFCQUFxQjs7O0lBQ2hDLHdDQUFnSDs7Ozs7SUFFaEgsb0RBZUU7O0lBRUYseUNBQTBCOztJQUMxQix5Q0FBMEI7O0lBQzFCLDZDQUE4Qjs7SUFDOUIsdUNBQTRDOztJQUM1Qyw0Q0FBOEM7O0lBQzlDLDJDQUF5RDs7SUFDekQsNENBQTBEOztJQVF4RCw0Q0FBK0QiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQge1xuICBDb21wb25lbnRGYWN0b3J5LFxuICBDb21wb25lbnRGYWN0b3J5UmVzb2x2ZXIsXG4gIERpcmVjdGl2ZSxcbiAgRWxlbWVudFJlZixcbiAgRXZlbnRFbWl0dGVyLFxuICBIb3N0LFxuICBJbnB1dCxcbiAgT25Jbml0LFxuICBPcHRpb25hbCxcbiAgT3V0cHV0LFxuICBSZW5kZXJlcjIsXG4gIFRlbXBsYXRlUmVmLFxuICBWaWV3Q29udGFpbmVyUmVmXG59IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBkaXN0aW5jdFVudGlsQ2hhbmdlZCB9IGZyb20gJ3J4anMvb3BlcmF0b3JzJztcblxuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgSW5wdXRCb29sZWFuIH0gZnJvbSAnLi4vY29yZS91dGlsL2NvbnZlcnQnO1xuaW1wb3J0IHsgTnpUb29sdGlwRGlyZWN0aXZlIH0gZnJvbSAnLi4vdG9vbHRpcC9uei10b29sdGlwLmRpcmVjdGl2ZSc7XG5pbXBvcnQgeyBOelBvcGNvbmZpcm1Db21wb25lbnQgfSBmcm9tICcuL256LXBvcGNvbmZpcm0uY29tcG9uZW50JztcblxuQERpcmVjdGl2ZSh7XG4gIHNlbGVjdG9yOiAnW256LXBvcGNvbmZpcm1dJyxcbiAgaG9zdDoge1xuICAgICdbY2xhc3MuYW50LXBvcG92ZXItb3Blbl0nOiAnaXNUb29sdGlwT3BlbidcbiAgfVxufSlcbmV4cG9ydCBjbGFzcyBOelBvcGNvbmZpcm1EaXJlY3RpdmUgZXh0ZW5kcyBOelRvb2x0aXBEaXJlY3RpdmUgaW1wbGVtZW50cyBPbkluaXQge1xuICBmYWN0b3J5OiBDb21wb25lbnRGYWN0b3J5PE56UG9wY29uZmlybUNvbXBvbmVudD4gPSB0aGlzLnJlc29sdmVyLnJlc29sdmVDb21wb25lbnRGYWN0b3J5KE56UG9wY29uZmlybUNvbXBvbmVudCk7XG5cbiAgcHJvdGVjdGVkIG5lZWRQcm94eVByb3BlcnRpZXMgPSBbXG4gICAgJ256VGl0bGUnLFxuICAgICduekNvbnRlbnQnLFxuICAgICduek92ZXJsYXlDbGFzc05hbWUnLFxuICAgICduek92ZXJsYXlTdHlsZScsXG4gICAgJ256TW91c2VFbnRlckRlbGF5JyxcbiAgICAnbnpNb3VzZUxlYXZlRGVsYXknLFxuICAgICduelZpc2libGUnLFxuICAgICduelRyaWdnZXInLFxuICAgICduelBsYWNlbWVudCcsXG4gICAgJ256T2tUZXh0JyxcbiAgICAnbnpPa1R5cGUnLFxuICAgICduekNhbmNlbFRleHQnLFxuICAgICduekNvbmRpdGlvbicsXG4gICAgJ256SWNvbidcbiAgXTtcblxuICBASW5wdXQoKSBuek9rVGV4dDogc3RyaW5nO1xuICBASW5wdXQoKSBuek9rVHlwZTogc3RyaW5nO1xuICBASW5wdXQoKSBuekNhbmNlbFRleHQ6IHN0cmluZztcbiAgQElucHV0KCkgbnpJY29uOiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgQElucHV0Qm9vbGVhbigpIG56Q29uZGl0aW9uOiBib29sZWFuO1xuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpPbkNhbmNlbCA9IG5ldyBFdmVudEVtaXR0ZXI8dm9pZD4oKTtcbiAgQE91dHB1dCgpIHJlYWRvbmx5IG56T25Db25maXJtID0gbmV3IEV2ZW50RW1pdHRlcjx2b2lkPigpO1xuXG4gIGNvbnN0cnVjdG9yKFxuICAgIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgaG9zdFZpZXc6IFZpZXdDb250YWluZXJSZWYsXG4gICAgcmVzb2x2ZXI6IENvbXBvbmVudEZhY3RvcnlSZXNvbHZlcixcbiAgICByZW5kZXJlcjogUmVuZGVyZXIyLFxuICAgIEBPcHRpb25hbCgpIHRvb2x0aXA6IE56UG9wY29uZmlybUNvbXBvbmVudCxcbiAgICBASG9zdCgpIEBPcHRpb25hbCgpIHB1YmxpYyBub0FuaW1hdGlvbj86IE56Tm9BbmltYXRpb25EaXJlY3RpdmVcbiAgKSB7XG4gICAgc3VwZXIoZWxlbWVudFJlZiwgaG9zdFZpZXcsIHJlc29sdmVyLCByZW5kZXJlciwgdG9vbHRpcCwgbm9BbmltYXRpb24pO1xuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgaWYgKCF0aGlzLnRvb2x0aXApIHtcbiAgICAgIGNvbnN0IHRvb2x0aXBDb21wb25lbnQgPSB0aGlzLmhvc3RWaWV3LmNyZWF0ZUNvbXBvbmVudCh0aGlzLmZhY3RvcnkpO1xuICAgICAgdGhpcy50b29sdGlwID0gdG9vbHRpcENvbXBvbmVudC5pbnN0YW5jZTtcbiAgICAgIHRoaXMudG9vbHRpcC5ub0FuaW1hdGlvbiA9IHRoaXMubm9BbmltYXRpb247XG4gICAgICAvLyBSZW1vdmUgZWxlbWVudCB3aGVuIHVzZSBkaXJlY3RpdmUgaHR0cHM6Ly9naXRodWIuY29tL05HLVpPUlJPL25nLXpvcnJvLWFudGQvaXNzdWVzLzE5NjdcbiAgICAgIHRoaXMucmVuZGVyZXIucmVtb3ZlQ2hpbGQoXG4gICAgICAgIHRoaXMucmVuZGVyZXIucGFyZW50Tm9kZSh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCksXG4gICAgICAgIHRvb2x0aXBDb21wb25lbnQubG9jYXRpb24ubmF0aXZlRWxlbWVudFxuICAgICAgKTtcbiAgICAgIHRoaXMuaXNEeW5hbWljVG9vbHRpcCA9IHRydWU7XG4gICAgICB0aGlzLm5lZWRQcm94eVByb3BlcnRpZXMuZm9yRWFjaChwcm9wZXJ0eSA9PiB0aGlzLnVwZGF0ZUNvbXBWYWx1ZShwcm9wZXJ0eSwgdGhpc1twcm9wZXJ0eV0pKTtcbiAgICAgIGNvbnN0IHZpc2libGVfID0gdGhpcy50b29sdGlwLm56VmlzaWJsZUNoYW5nZS5waXBlKGRpc3RpbmN0VW50aWxDaGFuZ2VkKCkpLnN1YnNjcmliZShkYXRhID0+IHtcbiAgICAgICAgdGhpcy52aXNpYmxlID0gZGF0YTtcbiAgICAgICAgdGhpcy5uelZpc2libGVDaGFuZ2UuZW1pdChkYXRhKTtcbiAgICAgIH0pO1xuICAgICAgY29uc3QgY2FuY2VsXyA9ICh0aGlzLnRvb2x0aXAgYXMgTnpQb3Bjb25maXJtQ29tcG9uZW50KS5uek9uQ2FuY2VsLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICAgIHRoaXMubnpPbkNhbmNlbC5lbWl0KCk7XG4gICAgICB9KTtcbiAgICAgIGNvbnN0IGNvbmZpcm1fID0gKHRoaXMudG9vbHRpcCBhcyBOelBvcGNvbmZpcm1Db21wb25lbnQpLm56T25Db25maXJtLnN1YnNjcmliZSgoKSA9PiB7XG4gICAgICAgIHRoaXMubnpPbkNvbmZpcm0uZW1pdCgpO1xuICAgICAgfSk7XG4gICAgICB0aGlzLnN1YnNfLmFkZCh2aXNpYmxlXyk7XG4gICAgICB0aGlzLnN1YnNfLmFkZChjYW5jZWxfKTtcbiAgICAgIHRoaXMuc3Vic18uYWRkKGNvbmZpcm1fKTtcbiAgICB9XG4gICAgdGhpcy50b29sdGlwLnNldE92ZXJsYXlPcmlnaW4odGhpcyk7XG4gIH1cbn1cbiJdfQ==