/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Directive, ElementRef, Inject, InjectionToken, Input, NgZone, Optional } from '@angular/core';
import { ANIMATION_MODULE_TYPE } from '@angular/platform-browser/animations';
import { NzWaveRenderer } from './nz-wave-renderer';
/**
 * @record
 */
export function NzWaveConfig() { }
if (false) {
    /** @type {?|undefined} */
    NzWaveConfig.prototype.disabled;
}
/** @type {?} */
export const NZ_WAVE_GLOBAL_DEFAULT_CONFIG = {
    disabled: false
};
/** @type {?} */
export const NZ_WAVE_GLOBAL_CONFIG = new InjectionToken('nz-wave-global-options', {
    providedIn: 'root',
    factory: NZ_WAVE_GLOBAL_CONFIG_FACTORY
});
/**
 * @return {?}
 */
export function NZ_WAVE_GLOBAL_CONFIG_FACTORY() {
    return NZ_WAVE_GLOBAL_DEFAULT_CONFIG;
}
export class NzWaveDirective {
    /**
     * @param {?} ngZone
     * @param {?} elementRef
     * @param {?} config
     * @param {?} animationType
     */
    constructor(ngZone, elementRef, config, animationType) {
        this.ngZone = ngZone;
        this.elementRef = elementRef;
        this.animationType = animationType;
        this.nzWaveExtraNode = false;
        this.waveDisabled = false;
        if (config && typeof config.disabled === 'boolean') {
            this.waveDisabled = config.disabled;
        }
        if (this.animationType === 'NoopAnimations') {
            this.waveDisabled = true;
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        if (this.waveRenderer) {
            this.waveRenderer.destroy();
        }
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        this.renderWaveIfEnabled();
    }
    /**
     * @return {?}
     */
    renderWaveIfEnabled() {
        if (!this.waveDisabled && this.elementRef.nativeElement) {
            this.waveRenderer = new NzWaveRenderer(this.elementRef.nativeElement, this.ngZone, this.nzWaveExtraNode);
        }
    }
}
NzWaveDirective.decorators = [
    { type: Directive, args: [{
                selector: '[nz-wave]'
            },] }
];
/** @nocollapse */
NzWaveDirective.ctorParameters = () => [
    { type: NgZone },
    { type: ElementRef },
    { type: undefined, decorators: [{ type: Optional }, { type: Inject, args: [NZ_WAVE_GLOBAL_CONFIG,] }] },
    { type: String, decorators: [{ type: Optional }, { type: Inject, args: [ANIMATION_MODULE_TYPE,] }] }
];
NzWaveDirective.propDecorators = {
    nzWaveExtraNode: [{ type: Input }]
};
if (false) {
    /** @type {?} */
    NzWaveDirective.prototype.nzWaveExtraNode;
    /**
     * @type {?}
     * @private
     */
    NzWaveDirective.prototype.waveRenderer;
    /**
     * @type {?}
     * @private
     */
    NzWaveDirective.prototype.waveDisabled;
    /**
     * @type {?}
     * @private
     */
    NzWaveDirective.prototype.ngZone;
    /**
     * @type {?}
     * @private
     */
    NzWaveDirective.prototype.elementRef;
    /**
     * @type {?}
     * @private
     */
    NzWaveDirective.prototype.animationType;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotd2F2ZS5kaXJlY3RpdmUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsiY29yZS93YXZlL256LXdhdmUuZGlyZWN0aXZlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQ0wsU0FBUyxFQUNULFVBQVUsRUFDVixNQUFNLEVBQ04sY0FBYyxFQUNkLEtBQUssRUFDTCxNQUFNLEVBR04sUUFBUSxFQUNULE1BQU0sZUFBZSxDQUFDO0FBQ3ZCLE9BQU8sRUFBRSxxQkFBcUIsRUFBRSxNQUFNLHNDQUFzQyxDQUFDO0FBQzdFLE9BQU8sRUFBRSxjQUFjLEVBQUUsTUFBTSxvQkFBb0IsQ0FBQzs7OztBQUVwRCxrQ0FFQzs7O0lBREMsZ0NBQW1COzs7QUFHckIsTUFBTSxPQUFPLDZCQUE2QixHQUFpQjtJQUN6RCxRQUFRLEVBQUUsS0FBSztDQUNoQjs7QUFFRCxNQUFNLE9BQU8scUJBQXFCLEdBQUcsSUFBSSxjQUFjLENBQWUsd0JBQXdCLEVBQUU7SUFDOUYsVUFBVSxFQUFFLE1BQU07SUFDbEIsT0FBTyxFQUFFLDZCQUE2QjtDQUN2QyxDQUFDOzs7O0FBRUYsTUFBTSxVQUFVLDZCQUE2QjtJQUMzQyxPQUFPLDZCQUE2QixDQUFDO0FBQ3ZDLENBQUM7QUFLRCxNQUFNLE9BQU8sZUFBZTs7Ozs7OztJQU0xQixZQUFvQixNQUFjLEVBQ2QsVUFBc0IsRUFDYSxNQUFvQixFQUNaLGFBQXFCO1FBSGhFLFdBQU0sR0FBTixNQUFNLENBQVE7UUFDZCxlQUFVLEdBQVYsVUFBVSxDQUFZO1FBRXFCLGtCQUFhLEdBQWIsYUFBYSxDQUFRO1FBUjNFLG9CQUFlLEdBQUcsS0FBSyxDQUFDO1FBR3pCLGlCQUFZLEdBQVksS0FBSyxDQUFDO1FBTXBDLElBQUksTUFBTSxJQUFJLE9BQU8sTUFBTSxDQUFDLFFBQVEsS0FBSyxTQUFTLEVBQUU7WUFDbEQsSUFBSSxDQUFDLFlBQVksR0FBRyxNQUFNLENBQUMsUUFBUSxDQUFDO1NBQ3JDO1FBQ0QsSUFBSSxJQUFJLENBQUMsYUFBYSxLQUFLLGdCQUFnQixFQUFFO1lBQzNDLElBQUksQ0FBQyxZQUFZLEdBQUcsSUFBSSxDQUFDO1NBQzFCO0lBQ0gsQ0FBQzs7OztJQUVELFdBQVc7UUFDVCxJQUFJLElBQUksQ0FBQyxZQUFZLEVBQUU7WUFDckIsSUFBSSxDQUFDLFlBQVksQ0FBQyxPQUFPLEVBQUUsQ0FBQztTQUM3QjtJQUNILENBQUM7Ozs7SUFFRCxRQUFRO1FBQ04sSUFBSSxDQUFDLG1CQUFtQixFQUFFLENBQUM7SUFDN0IsQ0FBQzs7OztJQUVELG1CQUFtQjtRQUNqQixJQUFJLENBQUMsSUFBSSxDQUFDLFlBQVksSUFBSSxJQUFJLENBQUMsVUFBVSxDQUFDLGFBQWEsRUFBRTtZQUN2RCxJQUFJLENBQUMsWUFBWSxHQUFHLElBQUksY0FBYyxDQUFDLElBQUksQ0FBQyxVQUFVLENBQUMsYUFBYSxFQUFFLElBQUksQ0FBQyxNQUFNLEVBQUUsSUFBSSxDQUFDLGVBQWUsQ0FBQyxDQUFDO1NBQzFHO0lBQ0gsQ0FBQzs7O1lBbkNGLFNBQVMsU0FBQztnQkFDVCxRQUFRLEVBQUUsV0FBVzthQUN0Qjs7OztZQTNCQyxNQUFNO1lBSk4sVUFBVTs0Q0F3Q0csUUFBUSxZQUFJLE1BQU0sU0FBQyxxQkFBcUI7eUNBQ3hDLFFBQVEsWUFBSSxNQUFNLFNBQUMscUJBQXFCOzs7OEJBUnBELEtBQUs7Ozs7SUFBTiwwQ0FBaUM7Ozs7O0lBRWpDLHVDQUFxQzs7Ozs7SUFDckMsdUNBQXNDOzs7OztJQUUxQixpQ0FBc0I7Ozs7O0lBQ3RCLHFDQUE4Qjs7Ozs7SUFFOUIsd0NBQXdFIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHtcbiAgRGlyZWN0aXZlLFxuICBFbGVtZW50UmVmLFxuICBJbmplY3QsXG4gIEluamVjdGlvblRva2VuLFxuICBJbnB1dCxcbiAgTmdab25lLFxuICBPbkRlc3Ryb3ksXG4gIE9uSW5pdCxcbiAgT3B0aW9uYWxcbn0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBBTklNQVRJT05fTU9EVUxFX1RZUEUgfSBmcm9tICdAYW5ndWxhci9wbGF0Zm9ybS1icm93c2VyL2FuaW1hdGlvbnMnO1xuaW1wb3J0IHsgTnpXYXZlUmVuZGVyZXIgfSBmcm9tICcuL256LXdhdmUtcmVuZGVyZXInO1xuXG5leHBvcnQgaW50ZXJmYWNlIE56V2F2ZUNvbmZpZyB7XG4gIGRpc2FibGVkPzogYm9vbGVhbjtcbn1cblxuZXhwb3J0IGNvbnN0IE5aX1dBVkVfR0xPQkFMX0RFRkFVTFRfQ09ORklHOiBOeldhdmVDb25maWcgPSB7XG4gIGRpc2FibGVkOiBmYWxzZVxufTtcblxuZXhwb3J0IGNvbnN0IE5aX1dBVkVfR0xPQkFMX0NPTkZJRyA9IG5ldyBJbmplY3Rpb25Ub2tlbjxOeldhdmVDb25maWc+KCduei13YXZlLWdsb2JhbC1vcHRpb25zJywge1xuICBwcm92aWRlZEluOiAncm9vdCcsXG4gIGZhY3Rvcnk6IE5aX1dBVkVfR0xPQkFMX0NPTkZJR19GQUNUT1JZXG59KTtcblxuZXhwb3J0IGZ1bmN0aW9uIE5aX1dBVkVfR0xPQkFMX0NPTkZJR19GQUNUT1JZKCk6IE56V2F2ZUNvbmZpZyB7XG4gIHJldHVybiBOWl9XQVZFX0dMT0JBTF9ERUZBVUxUX0NPTkZJRztcbn1cblxuQERpcmVjdGl2ZSh7XG4gIHNlbGVjdG9yOiAnW256LXdhdmVdJ1xufSlcbmV4cG9ydCBjbGFzcyBOeldhdmVEaXJlY3RpdmUgaW1wbGVtZW50cyBPbkluaXQsIE9uRGVzdHJveSB7XG4gIEBJbnB1dCgpIG56V2F2ZUV4dHJhTm9kZSA9IGZhbHNlO1xuXG4gIHByaXZhdGUgd2F2ZVJlbmRlcmVyOiBOeldhdmVSZW5kZXJlcjtcbiAgcHJpdmF0ZSB3YXZlRGlzYWJsZWQ6IGJvb2xlYW4gPSBmYWxzZTtcblxuICBjb25zdHJ1Y3Rvcihwcml2YXRlIG5nWm9uZTogTmdab25lLFxuICAgICAgICAgICAgICBwcml2YXRlIGVsZW1lbnRSZWY6IEVsZW1lbnRSZWYsXG4gICAgICAgICAgICAgIEBPcHRpb25hbCgpIEBJbmplY3QoTlpfV0FWRV9HTE9CQUxfQ09ORklHKSBjb25maWc6IE56V2F2ZUNvbmZpZyxcbiAgICAgICAgICAgICAgQE9wdGlvbmFsKCkgQEluamVjdChBTklNQVRJT05fTU9EVUxFX1RZUEUpIHByaXZhdGUgYW5pbWF0aW9uVHlwZTogc3RyaW5nKSB7XG4gICAgaWYgKGNvbmZpZyAmJiB0eXBlb2YgY29uZmlnLmRpc2FibGVkID09PSAnYm9vbGVhbicpIHtcbiAgICAgIHRoaXMud2F2ZURpc2FibGVkID0gY29uZmlnLmRpc2FibGVkO1xuICAgIH1cbiAgICBpZiAodGhpcy5hbmltYXRpb25UeXBlID09PSAnTm9vcEFuaW1hdGlvbnMnKSB7XG4gICAgICB0aGlzLndhdmVEaXNhYmxlZCA9IHRydWU7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgaWYgKHRoaXMud2F2ZVJlbmRlcmVyKSB7XG4gICAgICB0aGlzLndhdmVSZW5kZXJlci5kZXN0cm95KCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkluaXQoKTogdm9pZCB7XG4gICAgdGhpcy5yZW5kZXJXYXZlSWZFbmFibGVkKCk7XG4gIH1cblxuICByZW5kZXJXYXZlSWZFbmFibGVkKCk6IHZvaWQge1xuICAgIGlmICghdGhpcy53YXZlRGlzYWJsZWQgJiYgdGhpcy5lbGVtZW50UmVmLm5hdGl2ZUVsZW1lbnQpIHtcbiAgICAgIHRoaXMud2F2ZVJlbmRlcmVyID0gbmV3IE56V2F2ZVJlbmRlcmVyKHRoaXMuZWxlbWVudFJlZi5uYXRpdmVFbGVtZW50LCB0aGlzLm5nWm9uZSwgdGhpcy5ueldhdmVFeHRyYU5vZGUpO1xuICAgIH1cbiAgfVxufVxuIl19