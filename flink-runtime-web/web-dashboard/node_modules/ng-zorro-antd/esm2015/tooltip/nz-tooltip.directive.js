/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { ComponentFactoryResolver, Directive, ElementRef, EventEmitter, Host, Input, Optional, Output, Renderer2, ViewContainerRef } from '@angular/core';
import { Subscription } from 'rxjs';
import { distinctUntilChanged } from 'rxjs/operators';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { isNotNil } from '../core/util/check';
import { NzToolTipComponent } from './nz-tooltip.component';
export class NzTooltipDirective {
    // tslint:disable-line:no-any
    /**
     * @param {?} elementRef
     * @param {?} hostView
     * @param {?} resolver
     * @param {?} renderer
     * @param {?} tooltip
     * @param {?=} noAnimation
     */
    constructor(elementRef, hostView, resolver, renderer, tooltip, noAnimation) {
        this.elementRef = elementRef;
        this.hostView = hostView;
        this.resolver = resolver;
        this.renderer = renderer;
        this.tooltip = tooltip;
        this.noAnimation = noAnimation;
        // [NOTE] Here hard coded, and nzTitle used only under NzTooltipDirective currently.
        this.isTooltipOpen = false;
        this.isDynamicTooltip = false; // Indicate whether current tooltip is dynamic created
        this.factory = this.resolver.resolveComponentFactory(NzToolTipComponent);
        /**
         * Names of properties that should be proxy to child component.
         */
        this.needProxyProperties = [
            'nzTitle',
            'nzContent',
            'nzOverlayClassName',
            'nzOverlayStyle',
            'nzMouseEnterDelay',
            'nzMouseLeaveDelay',
            'nzVisible',
            'nzTrigger',
            'nzPlacement'
        ];
        this.subs_ = new Subscription();
        this.nzVisibleChange = new EventEmitter();
    }
    /**
     * @param {?} title
     * @return {?}
     */
    set setTitle(title) {
        this.nzTitle = title;
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        this.updateProxies(changes);
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        // Support faster tooltip mode: <a nz-tooltip="xxx"></a>. [NOTE] Used only under NzTooltipDirective currently.
        if (!this.tooltip) {
            /** @type {?} */
            const tooltipComponent = this.hostView.createComponent(this.factory);
            this.tooltip = tooltipComponent.instance;
            this.tooltip.noAnimation = this.noAnimation;
            // Remove element when use directive https://github.com/NG-ZORRO/ng-zorro-antd/issues/1967
            this.renderer.removeChild(this.renderer.parentNode(this.elementRef.nativeElement), tooltipComponent.location.nativeElement);
            this.isDynamicTooltip = true;
            this.needProxyProperties.forEach((/**
             * @param {?} property
             * @return {?}
             */
            property => this.updateCompValue(property, this[property])));
            /** @type {?} */
            const visible_ = this.tooltip.nzVisibleChange.pipe(distinctUntilChanged()).subscribe((/**
             * @param {?} data
             * @return {?}
             */
            data => {
                this.visible = data;
                this.nzVisibleChange.emit(data);
            }));
            this.subs_.add(visible_);
        }
        this.tooltip.setOverlayOrigin(this);
    }
    /**
     * @return {?}
     */
    ngAfterViewInit() {
        if (this.tooltip.nzTrigger === 'hover') {
            /** @type {?} */
            let overlayElement;
            this.renderer.listen(this.elementRef.nativeElement, 'mouseenter', (/**
             * @return {?}
             */
            () => this.delayEnterLeave(true, true, this.tooltip.nzMouseEnterDelay)));
            this.renderer.listen(this.elementRef.nativeElement, 'mouseleave', (/**
             * @return {?}
             */
            () => {
                this.delayEnterLeave(true, false, this.tooltip.nzMouseLeaveDelay);
                if (this.tooltip.overlay.overlayRef && !overlayElement) {
                    // NOTE: we bind events under "mouseleave" due to the overlayRef is only created after the overlay was completely shown up
                    overlayElement = this.tooltip.overlay.overlayRef.overlayElement;
                    this.renderer.listen(overlayElement, 'mouseenter', (/**
                     * @return {?}
                     */
                    () => this.delayEnterLeave(false, true)));
                    this.renderer.listen(overlayElement, 'mouseleave', (/**
                     * @return {?}
                     */
                    () => this.delayEnterLeave(false, false)));
                }
            }));
        }
        else if (this.tooltip.nzTrigger === 'focus') {
            this.renderer.listen(this.elementRef.nativeElement, 'focus', (/**
             * @return {?}
             */
            () => this.show()));
            this.renderer.listen(this.elementRef.nativeElement, 'blur', (/**
             * @return {?}
             */
            () => this.hide()));
        }
        else if (this.tooltip.nzTrigger === 'click') {
            this.renderer.listen(this.elementRef.nativeElement, 'click', (/**
             * @param {?} e
             * @return {?}
             */
            e => {
                e.preventDefault();
                this.show();
            }));
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.subs_.unsubscribe();
    }
    // tslint:disable-next-line:no-any
    /**
     * @protected
     * @param {?} key
     * @param {?} value
     * @return {?}
     */
    updateCompValue(key, value) {
        if (this.isDynamicTooltip && isNotNil(value)) {
            this.tooltip[key] = value;
        }
    }
    /**
     * @private
     * @return {?}
     */
    show() {
        this.tooltip.show();
        this.isTooltipOpen = true;
    }
    /**
     * @private
     * @return {?}
     */
    hide() {
        this.tooltip.hide();
        this.isTooltipOpen = false;
    }
    /**
     * @private
     * @param {?} isOrigin
     * @param {?} isEnter
     * @param {?=} delay
     * @return {?}
     */
    delayEnterLeave(isOrigin, isEnter, delay = -1) {
        if (this.delayTimer) {
            // Clear timer during the delay time
            clearTimeout(this.delayTimer);
            this.delayTimer = null;
        }
        else if (delay > 0) {
            this.delayTimer = setTimeout((/**
             * @return {?}
             */
            () => {
                this.delayTimer = null;
                isEnter ? this.show() : this.hide();
            }), delay * 1000);
        }
        else {
            isEnter && isOrigin ? this.show() : this.hide(); // [Compatible] The "isOrigin" is used due to the tooltip will not hide immediately (may caused by the fade-out animation)
        }
    }
    /**
     * Set inputs of child components when this component's inputs change.
     * @private
     * @param {?} changes
     * @return {?}
     */
    updateProxies(changes) {
        if (this.tooltip) {
            Object.keys(changes).forEach((/**
             * @param {?} key
             * @return {?}
             */
            key => {
                /** @type {?} */
                const change = changes[key];
                if (change) {
                    this.updateCompValue(key, change.currentValue);
                }
            }));
            if (changes.setTitle) {
                this.nzTitle = changes.setTitle.currentValue;
                this.updateCompValue('nzTitle', changes.setTitle.currentValue);
            }
            this.tooltip.cdr.markForCheck(); // Manually trigger change detection of component.
        }
    }
}
NzTooltipDirective.decorators = [
    { type: Directive, args: [{
                selector: '[nz-tooltip]',
                host: {
                    '[class.ant-tooltip-open]': 'isTooltipOpen'
                }
            },] }
];
/** @nocollapse */
NzTooltipDirective.ctorParameters = () => [
    { type: ElementRef },
    { type: ViewContainerRef },
    { type: ComponentFactoryResolver },
    { type: Renderer2 },
    { type: NzToolTipComponent, decorators: [{ type: Optional }] },
    { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
];
NzTooltipDirective.propDecorators = {
    nzVisibleChange: [{ type: Output }],
    nzTitle: [{ type: Input, args: ['nz-tooltip',] }],
    setTitle: [{ type: Input, args: ['nzTitle',] }],
    nzContent: [{ type: Input }],
    nzMouseEnterDelay: [{ type: Input }],
    nzMouseLeaveDelay: [{ type: Input }],
    nzOverlayClassName: [{ type: Input }],
    nzOverlayStyle: [{ type: Input }],
    nzTrigger: [{ type: Input }],
    nzVisible: [{ type: Input }],
    nzPlacement: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzTooltipDirective.prototype.isTooltipOpen;
    /** @type {?} */
    NzTooltipDirective.prototype.isDynamicTooltip;
    /** @type {?} */
    NzTooltipDirective.prototype.delayTimer;
    /** @type {?} */
    NzTooltipDirective.prototype.visible;
    /** @type {?} */
    NzTooltipDirective.prototype.factory;
    /**
     * Names of properties that should be proxy to child component.
     * @type {?}
     * @protected
     */
    NzTooltipDirective.prototype.needProxyProperties;
    /**
     * @type {?}
     * @protected
     */
    NzTooltipDirective.prototype.subs_;
    /** @type {?} */
    NzTooltipDirective.prototype.nzVisibleChange;
    /** @type {?} */
    NzTooltipDirective.prototype.nzTitle;
    /** @type {?} */
    NzTooltipDirective.prototype.nzContent;
    /** @type {?} */
    NzTooltipDirective.prototype.nzMouseEnterDelay;
    /** @type {?} */
    NzTooltipDirective.prototype.nzMouseLeaveDelay;
    /** @type {?} */
    NzTooltipDirective.prototype.nzOverlayClassName;
    /** @type {?} */
    NzTooltipDirective.prototype.nzOverlayStyle;
    /** @type {?} */
    NzTooltipDirective.prototype.nzTrigger;
    /** @type {?} */
    NzTooltipDirective.prototype.nzVisible;
    /** @type {?} */
    NzTooltipDirective.prototype.nzPlacement;
    /** @type {?} */
    NzTooltipDirective.prototype.elementRef;
    /** @type {?} */
    NzTooltipDirective.prototype.hostView;
    /** @type {?} */
    NzTooltipDirective.prototype.resolver;
    /** @type {?} */
    NzTooltipDirective.prototype.renderer;
    /** @type {?} */
    NzTooltipDirective.prototype.tooltip;
    /** @type {?} */
    NzTooltipDirective.prototype.noAnimation;
    /* Skipping unhandled member: [property: string]: any;*/
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdG9vbHRpcC5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidG9vbHRpcC9uei10b29sdGlwLmRpcmVjdGl2ZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUdMLHdCQUF3QixFQUN4QixTQUFTLEVBQ1QsVUFBVSxFQUNWLFlBQVksRUFDWixJQUFJLEVBQ0osS0FBSyxFQUlMLFFBQVEsRUFDUixNQUFNLEVBQ04sU0FBUyxFQUdULGdCQUFnQixFQUNqQixNQUFNLGVBQWUsQ0FBQztBQUV2QixPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sTUFBTSxDQUFDO0FBQ3BDLE9BQU8sRUFBRSxvQkFBb0IsRUFBRSxNQUFNLGdCQUFnQixDQUFDO0FBRXRELE9BQU8sRUFBRSxzQkFBc0IsRUFBRSxNQUFNLGdEQUFnRCxDQUFDO0FBQ3hGLE9BQU8sRUFBRSxRQUFRLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQztBQUU5QyxPQUFPLEVBQUUsa0JBQWtCLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQVE1RCxNQUFNLE9BQU8sa0JBQWtCOzs7Ozs7Ozs7O0lBMEM3QixZQUNTLFVBQXNCLEVBQ3RCLFFBQTBCLEVBQzFCLFFBQWtDLEVBQ2xDLFFBQW1CLEVBQ1AsT0FBMkIsRUFDbkIsV0FBb0M7UUFMeEQsZUFBVSxHQUFWLFVBQVUsQ0FBWTtRQUN0QixhQUFRLEdBQVIsUUFBUSxDQUFrQjtRQUMxQixhQUFRLEdBQVIsUUFBUSxDQUEwQjtRQUNsQyxhQUFRLEdBQVIsUUFBUSxDQUFXO1FBQ1AsWUFBTyxHQUFQLE9BQU8sQ0FBb0I7UUFDbkIsZ0JBQVcsR0FBWCxXQUFXLENBQXlCOztRQTlDakUsa0JBQWEsR0FBWSxLQUFLLENBQUM7UUFDL0IscUJBQWdCLEdBQUcsS0FBSyxDQUFDLENBQUMsc0RBQXNEO1FBR2hGLFlBQU8sR0FBeUMsSUFBSSxDQUFDLFFBQVEsQ0FBQyx1QkFBdUIsQ0FBQyxrQkFB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TCxLQUFLO3dCQUNMLEtBQUs7MEJBQ0wsS0FBSzs7OztJQXBDTiwyQ0FBK0I7O0lBQy9CLDhDQUF5Qjs7SUFDekIsd0NBQTBCOztJQUMxQixxQ0FBaUI7O0lBQ2pCLHFDQUEwRzs7Ozs7O0lBRzFHLGlEQVVFOzs7OztJQUVGLG1DQUFxQzs7SUFFckMsNkNBQWlFOztJQUVqRSxxQ0FBZ0U7O0lBTWhFLHVDQUErQzs7SUFDL0MsK0NBQW1DOztJQUNuQywrQ0FBbUM7O0lBQ25DLGdEQUFvQzs7SUFDcEMsNENBQW1EOztJQUNuRCx1Q0FBMkI7O0lBQzNCLHVDQUE0Qjs7SUFDNUIseUNBQTZCOztJQUszQix3Q0FBNkI7O0lBQzdCLHNDQUFpQzs7SUFDakMsc0NBQXlDOztJQUN6QyxzQ0FBMEI7O0lBQzFCLHFDQUE4Qzs7SUFDOUMseUNBQStEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgQWZ0ZXJWaWV3SW5pdCxcbiAgQ29tcG9uZW50RmFjdG9yeSxcbiAgQ29tcG9uZW50RmFjdG9yeVJlc29sdmVyLFxuICBEaXJlY3RpdmUsXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSG9zdCxcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFJlbmRlcmVyMixcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVGVtcGxhdGVSZWYsXG4gIFZpZXdDb250YWluZXJSZWZcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5cbmltcG9ydCB7IFN1YnNjcmlwdGlvbiB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgZGlzdGluY3RVbnRpbENoYW5nZWQgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5cbmltcG9ydCB7IE56Tm9BbmltYXRpb25EaXJlY3RpdmUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24uZGlyZWN0aXZlJztcbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcblxuaW1wb3J0IHsgTnpUb29sVGlwQ29tcG9uZW50IH0gZnJvbSAnLi9uei10b29sdGlwLmNvbXBvbmVudCc7XG5cbkBEaXJlY3RpdmUoe1xuICBzZWxlY3RvcjogJ1tuei10b29sdGlwXScsXG4gIGhvc3Q6IHtcbiAgICAnW2NsYXNzLmFudC10b29sdGlwLW9wZW5dJzogJ2lzVG9vbHRpcE9wZW4nXG4gIH1cbn0pXG5leHBvcnQgY2xhc3MgTnpUb29sdGlwRGlyZWN0aXZlIGltcGxlbWVudHMgQWZ0ZXJWaWV3SW5pdCwgT25DaGFuZ2VzLCBPbkluaXQsIE9uRGVzdHJveSB7XG4gIC8vIFtOT1RFXSBIZXJlIGhhcmQgY29kZWQsIGFuZCBuelRpdGxlIHVzZWQgb25seSB1bmRlciBOelRvb2x0aXBEaXJlY3RpdmUgY3VycmVudGx5LlxuICBpc1Rvb2x0aXBPcGVuOiBib29sZWFuID0gZmFsc2U7XG4gIGlzRHluYW1pY1Rvb2x0aXAgPSBmYWxzZTsgLy8gSW5kaWNhdGUgd2hldGhlciBjdXJyZW50IHRvb2x0aXAgaXMgZHluYW1pYyBjcmVhdGVkXG4gIGRlbGF5VGltZXI6IG51bWJlciB8IG51bGw7IC8vIFRpbWVyIGZvciBkZWxheSBlbnRlci9sZWF2ZVxuICB2aXNpYmxlOiBib29sZWFuO1xuICBmYWN0b3J5OiBDb21wb25lbnRGYWN0b3J5PE56VG9vbFRpcENvbXBvbmVudD4gPSB0aGlzLnJlc29sdmVyLnJlc29sdmVDb21wb25lbnRGYWN0b3J5KE56VG9vbFRpcENvbXBvbmVudCk7XG5cbiAgLyoqIE5hbWVzIG9mIHByb3BlcnRpZXMgdGhhdCBzaG91bGQgYmUgcHJveHkgdG8gY2hpbGQgY29tcG9uZW50LiAqL1xuICBwcm90ZWN0ZWQgbmVlZFByb3h5UHJvcGVydGllcyA9IFtcbiAgICAnbnpUaXRsZScsXG4gICAgJ256Q29udGVudCcsXG4gICAgJ256T3ZlcmxheUNsYXNzTmFtZScsXG4gICAgJ256T3ZlcmxheVN0eWxlJyxcbiAgICAnbnpNb3VzZUVudGVyRGVsYXknLFxuICAgICduek1vdXNlTGVhdmVEZWxheScsXG4gICAgJ256VmlzaWJsZScsXG4gICAgJ256VHJpZ2dlcicsXG4gICAgJ256UGxhY2VtZW50J1xuICBdO1xuXG4gIHByb3RlY3RlZCBzdWJzXyA9IG5ldyBTdWJzY3JpcHRpb24oKTtcblxuICBAT3V0cHV0KCkgcmVhZG9ubHkgbnpWaXNpYmxlQ2hhbmdlID0gbmV3IEV2ZW50RW1pdHRlcjxib29sZWFuPigpO1xuXG4gIEBJbnB1dCgnbnotdG9vbHRpcCcpIG56VGl0bGU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+IHwgbnVsbDtcblxuICBASW5wdXQoJ256VGl0bGUnKSBzZXQgc2V0VGl0bGUodGl0bGU6IHN0cmluZyB8IFRlbXBsYXRlUmVmPHZvaWQ+IHwgbnVsbCkge1xuICAgIHRoaXMubnpUaXRsZSA9IHRpdGxlO1xuICB9XG5cbiAgQElucHV0KCkgbnpDb250ZW50OiBzdHJpbmcgfCBUZW1wbGF0ZVJlZjx2b2lkPjtcbiAgQElucHV0KCkgbnpNb3VzZUVudGVyRGVsYXk6IG51bWJlcjtcbiAgQElucHV0KCkgbnpNb3VzZUxlYXZlRGVsYXk6IG51bWJlcjtcbiAgQElucHV0KCkgbnpPdmVybGF5Q2xhc3NOYW1lOiBzdHJpbmc7XG4gIEBJbnB1dCgpIG56T3ZlcmxheVN0eWxlOiB7IFtrZXk6IHN0cmluZ106IHN0cmluZyB9O1xuICBASW5wdXQoKSBuelRyaWdnZXI6IHN0cmluZztcbiAgQElucHV0KCkgbnpWaXNpYmxlOiBib29sZWFuO1xuICBASW5wdXQoKSBuelBsYWNlbWVudDogc3RyaW5nO1xuXG4gIFtwcm9wZXJ0eTogc3RyaW5nXTogYW55OyAvLyB0c2xpbnQ6ZGlzYWJsZS1saW5lOm5vLWFueVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHB1YmxpYyBlbGVtZW50UmVmOiBFbGVtZW50UmVmLFxuICAgIHB1YmxpYyBob3N0VmlldzogVmlld0NvbnRhaW5lclJlZixcbiAgICBwdWJsaWMgcmVzb2x2ZXI6IENvbXBvbmVudEZhY3RvcnlSZXNvbHZlcixcbiAgICBwdWJsaWMgcmVuZGVyZXI6IFJlbmRlcmVyMixcbiAgICBAT3B0aW9uYWwoKSBwdWJsaWMgdG9vbHRpcDogTnpUb29sVGlwQ29tcG9uZW50LFxuICAgIEBIb3N0KCkgQE9wdGlvbmFsKCkgcHVibGljIG5vQW5pbWF0aW9uPzogTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZVxuICApIHt9XG5cbiAgbmdPbkNoYW5nZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIHRoaXMudXBkYXRlUHJveGllcyhjaGFuZ2VzKTtcbiAgfVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIC8vIFN1cHBvcnQgZmFzdGVyIHRvb2x0aXAgbW9kZTogPGEgbnotdG9vbHRpcD1cInh4eFwiPjwvYT4uIFtOT1RFXSBVc2VkIG9ubHkgdW5kZXIgTnpUb29sdGlwRGlyZWN0aXZlIGN1cnJlbnRseS5cbiAgICBpZiAoIXRoaXMudG9vbHRpcCkge1xuICAgICAgY29uc3QgdG9vbHRpcENvbXBvbmVudCA9IHRoaXMuaG9zdFZpZXcuY3JlYXRlQ29tcG9uZW50KHRoaXMuZmFjdG9yeSk7XG4gICAgICB0aGlzLnRvb2x0aXAgPSB0b29sdGlwQ29tcG9uZW50Lmluc3RhbmNlO1xuICAgICAgdGhpcy50b29sdGlwLm5vQW5pbWF0aW9uID0gdGhpcy5ub0FuaW1hdGlvbjtcbiAgICAgIC8vIFJlbW92ZSBlbGVtZW50IHdoZW4gdXNlIGRpcmVjdGl2ZSBodHRwczovL2dpdGh1Yi5jb20vTkctWk9SUk8vbmctem9ycm8tYW50ZC9pc3N1ZXMvMTk2N1xuICAgICAgdGhpcy5yZW5kZXJlci5yZW1vdmVDaGlsZChcbiAgICAgICAgdGhpcy5yZW5kZXJlci5wYXJlbnROb2RlKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50KSxcbiAgICAgICAgdG9vbHRpcENvbXBvbmVudC5sb2NhdGlvbi5uYXRpdmVFbGVtZW50XG4gICAgICApO1xuICAgICAgdGhpcy5pc0R5bmFtaWNUb29sdGlwID0gdHJ1ZTtcbiAgICAgIHRoaXMubmVlZFByb3h5UHJvcGVydGllcy5mb3JFYWNoKHByb3BlcnR5ID0+IHRoaXMudXBkYXRlQ29tcFZhbHVlKHByb3BlcnR5LCB0aGlzW3Byb3BlcnR5XSkpO1xuICAgICAgY29uc3QgdmlzaWJsZV8gPSB0aGlzLnRvb2x0aXAubnpWaXNpYmxlQ2hhbmdlLnBpcGUoZGlzdGluY3RVbnRpbENoYW5nZWQoKSkuc3Vic2NyaWJlKGRhdGEgPT4ge1xuICAgICAgICB0aGlzLnZpc2libGUgPSBkYXRhO1xuICAgICAgICB0aGlzLm56VmlzaWJsZUNoYW5nZS5lbWl0KGRhdGEpO1xuICAgICAgfSk7XG4gICAgICB0aGlzLnN1YnNfLmFkZCh2aXNpYmxlXyk7XG4gICAgfVxuICAgIHRoaXMudG9vbHRpcC5zZXRPdmVybGF5T3JpZ2luKHRoaXMpO1xuICB9XG5cbiAgbmdBZnRlclZpZXdJbml0KCk6IHZvaWQge1xuICAgIGlmICh0aGlzLnRvb2x0aXAubnpUcmlnZ2VyID09PSAnaG92ZXInKSB7XG4gICAgICBsZXQgb3ZlcmxheUVsZW1lbnQ6IEhUTUxFbGVtZW50O1xuICAgICAgdGhpcy5yZW5kZXJlci5saXN0ZW4odGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdtb3VzZWVudGVyJywgKCkgPT5cbiAgICAgICAgdGhpcy5kZWxheUVudGVyTGVhdmUodHJ1ZSwgdHJ1ZSwgdGhpcy50b29sdGlwLm56TW91c2VFbnRlckRlbGF5KVxuICAgICAgKTtcbiAgICAgIHRoaXMucmVuZGVyZXIubGlzdGVuKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCAnbW91c2VsZWF2ZScsICgpID0+IHtcbiAgICAgICAgdGhpcy5kZWxheUVudGVyTGVhdmUodHJ1ZSwgZmFsc2UsIHRoaXMudG9vbHRpcC5uek1vdXNlTGVhdmVEZWxheSk7XG4gICAgICAgIGlmICh0aGlzLnRvb2x0aXAub3ZlcmxheS5vdmVybGF5UmVmICYmICFvdmVybGF5RWxlbWVudCkge1xuICAgICAgICAgIC8vIE5PVEU6IHdlIGJpbmQgZXZlbnRzIHVuZGVyIFwibW91c2VsZWF2ZVwiIGR1ZSB0byB0aGUgb3ZlcmxheVJlZiBpcyBvbmx5IGNyZWF0ZWQgYWZ0ZXIgdGhlIG92ZXJsYXkgd2FzIGNvbXBsZXRlbHkgc2hvd24gdXBcbiAgICAgICAgICBvdmVybGF5RWxlbWVudCA9IHRoaXMudG9vbHRpcC5vdmVybGF5Lm92ZXJsYXlSZWYub3ZlcmxheUVsZW1lbnQ7XG4gICAgICAgICAgdGhpcy5yZW5kZXJlci5saXN0ZW4ob3ZlcmxheUVsZW1lbnQsICdtb3VzZWVudGVyJywgKCkgPT4gdGhpcy5kZWxheUVudGVyTGVhdmUoZmFsc2UsIHRydWUpKTtcbiAgICAgICAgICB0aGlzLnJlbmRlcmVyLmxpc3RlbihvdmVybGF5RWxlbWVudCwgJ21vdXNlbGVhdmUnLCAoKSA9PiB0aGlzLmRlbGF5RW50ZXJMZWF2ZShmYWxzZSwgZmFsc2UpKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfSBlbHNlIGlmICh0aGlzLnRvb2x0aXAubnpUcmlnZ2VyID09PSAnZm9jdXMnKSB7XG4gICAgICB0aGlzLnJlbmRlcmVyLmxpc3Rlbih0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwgJ2ZvY3VzJywgKCkgPT4gdGhpcy5zaG93KCkpO1xuICAgICAgdGhpcy5yZW5kZXJlci5saXN0ZW4odGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdibHVyJywgKCkgPT4gdGhpcy5oaWRlKCkpO1xuICAgIH0gZWxzZSBpZiAodGhpcy50b29sdGlwLm56VHJpZ2dlciA9PT0gJ2NsaWNrJykge1xuICAgICAgdGhpcy5yZW5kZXJlci5saXN0ZW4odGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQsICdjbGljaycsIGUgPT4ge1xuICAgICAgICBlLnByZXZlbnREZWZhdWx0KCk7XG4gICAgICAgIHRoaXMuc2hvdygpO1xuICAgICAgfSk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5zdWJzXy51bnN1YnNjcmliZSgpO1xuICB9XG5cbiAgLy8gdHNsaW50OmRpc2FibGUtbmV4dC1saW5lOm5vLWFueVxuICBwcm90ZWN0ZWQgdXBkYXRlQ29tcFZhbHVlKGtleTogc3RyaW5nLCB2YWx1ZTogYW55KTogdm9pZCB7XG4gICAgaWYgKHRoaXMuaXNEeW5hbWljVG9vbHRpcCAmJiBpc05vdE5pbCh2YWx1ZSkpIHtcbiAgICAgIHRoaXMudG9vbHRpcFtrZXldID0gdmFsdWU7XG4gICAgfVxuICB9XG5cbiAgcHJpdmF0ZSBzaG93KCk6IHZvaWQge1xuICAgIHRoaXMudG9vbHRpcC5zaG93KCk7XG4gICAgdGhpcy5pc1Rvb2x0aXBPcGVuID0gdHJ1ZTtcbiAgfVxuXG4gIHByaXZhdGUgaGlkZSgpOiB2b2lkIHtcbiAgICB0aGlzLnRvb2x0aXAuaGlkZSgpO1xuICAgIHRoaXMuaXNUb29sdGlwT3BlbiA9IGZhbHNlO1xuICB9XG5cbiAgcHJpdmF0ZSBkZWxheUVudGVyTGVhdmUoaXNPcmlnaW46IGJvb2xlYW4sIGlzRW50ZXI6IGJvb2xlYW4sIGRlbGF5OiBudW1iZXIgPSAtMSk6IHZvaWQge1xuICAgIGlmICh0aGlzLmRlbGF5VGltZXIpIHtcbiAgICAgIC8vIENsZWFyIHRpbWVyIGR1cmluZyB0aGUgZGVsYXkgdGltZVxuICAgICAgY2xlYXJUaW1lb3V0KHRoaXMuZGVsYXlUaW1lcik7XG4gICAgICB0aGlzLmRlbGF5VGltZXIgPSBudWxsO1xuICAgIH0gZWxzZSBpZiAoZGVsYXkgPiAwKSB7XG4gICAgICB0aGlzLmRlbGF5VGltZXIgPSBzZXRUaW1lb3V0KCgpID0+IHtcbiAgICAgICAgdGhpcy5kZWxheVRpbWVyID0gbnVsbDtcbiAgICAgICAgaXNFbnRlciA/IHRoaXMuc2hvdygpIDogdGhpcy5oaWRlKCk7XG4gICAgICB9LCBkZWxheSAqIDEwMDApO1xuICAgIH0gZWxzZSB7XG4gICAgICBpc0VudGVyICYmIGlzT3JpZ2luID8gdGhpcy5zaG93KCkgOiB0aGlzLmhpZGUoKTsgLy8gW0NvbXBhdGlibGVdIFRoZSBcImlzT3JpZ2luXCIgaXMgdXNlZCBkdWUgdG8gdGhlIHRvb2x0aXAgd2lsbCBub3QgaGlkZSBpbW1lZGlhdGVseSAobWF5IGNhdXNlZCBieSB0aGUgZmFkZS1vdXQgYW5pbWF0aW9uKVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBTZXQgaW5wdXRzIG9mIGNoaWxkIGNvbXBvbmVudHMgd2hlbiB0aGlzIGNvbXBvbmVudCdzIGlucHV0cyBjaGFuZ2UuXG4gICAqIEBwYXJhbSBjaGFuZ2VzXG4gICAqL1xuICBwcml2YXRlIHVwZGF0ZVByb3hpZXMoY2hhbmdlczogU2ltcGxlQ2hhbmdlcyk6IHZvaWQge1xuICAgIGlmICh0aGlzLnRvb2x0aXApIHtcbiAgICAgIE9iamVjdC5rZXlzKGNoYW5nZXMpLmZvckVhY2goa2V5ID0+IHtcbiAgICAgICAgY29uc3QgY2hhbmdlID0gY2hhbmdlc1trZXldO1xuICAgICAgICBpZiAoY2hhbmdlKSB7XG4gICAgICAgICAgdGhpcy51cGRhdGVDb21wVmFsdWUoa2V5LCBjaGFuZ2UuY3VycmVudFZhbHVlKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG5cbiAgICAgIGlmIChjaGFuZ2VzLnNldFRpdGxlKSB7XG4gICAgICAgIHRoaXMubnpUaXRsZSA9IGNoYW5nZXMuc2V0VGl0bGUuY3VycmVudFZhbHVlO1xuICAgICAgICB0aGlzLnVwZGF0ZUNvbXBWYWx1ZSgnbnpUaXRsZScsIGNoYW5nZXMuc2V0VGl0bGUuY3VycmVudFZhbHVlKTtcbiAgICAgIH1cblxuICAgICAgdGhpcy50b29sdGlwLmNkci5tYXJrRm9yQ2hlY2soKTsgLy8gTWFudWFsbHkgdHJpZ2dlciBjaGFuZ2UgZGV0ZWN0aW9uIG9mIGNvbXBvbmVudC5cbiAgICB9XG4gIH1cbn1cbiJdfQ==