/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import * as tslib_1 from "tslib";
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { ChangeDetectionStrategy, ChangeDetectorRef, Component, ContentChildren, ElementRef, EventEmitter, Host, Input, Optional, Output, QueryList, ViewChild, ViewEncapsulation } from '@angular/core';
import { combineLatest, merge, Subject } from 'rxjs';
import { flatMap, map, startWith, takeUntil } from 'rxjs/operators';
import { collapseMotion } from '../core/animation/collapse';
import { slideMotion } from '../core/animation/slide';
import { zoomBigMotion } from '../core/animation/zoom';
import { NzNoAnimationDirective } from '../core/no-animation/nz-no-animation.directive';
import { getPlacementName, DEFAULT_SUBMENU_POSITIONS, POSITION_MAP } from '../core/overlay/overlay-position';
import { NzUpdateHostClassService } from '../core/services/update-host-class.service';
import { InputBoolean } from '../core/util/convert';
import { NzMenuItemDirective } from './nz-menu-item.directive';
import { NzMenuService } from './nz-menu.service';
import { NzSubmenuService } from './nz-submenu.service';
export class NzSubMenuComponent {
    /**
     * @param {?} elementRef
     * @param {?} nzMenuService
     * @param {?} cdr
     * @param {?} nzSubmenuService
     * @param {?} nzUpdateHostClassService
     * @param {?=} noAnimation
     */
    constructor(elementRef, nzMenuService, cdr, nzSubmenuService, nzUpdateHostClassService, noAnimation) {
        this.elementRef = elementRef;
        this.nzMenuService = nzMenuService;
        this.cdr = cdr;
        this.nzSubmenuService = nzSubmenuService;
        this.nzUpdateHostClassService = nzUpdateHostClassService;
        this.noAnimation = noAnimation;
        this.nzOpen = false;
        this.nzDisabled = false;
        this.nzOpenChange = new EventEmitter();
        this.placement = 'rightTop';
        this.expandState = 'collapsed';
        this.overlayPositions = [...DEFAULT_SUBMENU_POSITIONS];
        this.destroy$ = new Subject();
        this.isChildMenuSelected = false;
        this.isMouseHover = false;
    }
    /**
     * @param {?} open
     * @return {?}
     */
    setOpenState(open) {
        this.nzSubmenuService.setOpenState(open);
    }
    /**
     * @return {?}
     */
    clickSubMenuTitle() {
        if (this.nzSubmenuService.mode === 'inline' && !this.nzMenuService.isInDropDown && !this.nzDisabled) {
            this.setOpenState(!this.nzOpen);
        }
    }
    /**
     * @param {?} value
     * @return {?}
     */
    setMouseEnterState(value) {
        this.isMouseHover = value;
        this.setClassMap();
        this.nzSubmenuService.setMouseEnterState(value);
    }
    /**
     * @return {?}
     */
    setTriggerWidth() {
        if (this.nzSubmenuService.mode === 'horizontal') {
            this.triggerWidth = this.cdkOverlayOrigin.nativeElement.getBoundingClientRect().width;
        }
    }
    /**
     * @param {?} position
     * @return {?}
     */
    onPositionChange(position) {
        this.placement = (/** @type {?} */ (getPlacementName(position)));
        this.cdr.markForCheck();
    }
    /**
     * @return {?}
     */
    setClassMap() {
        /** @type {?} */
        const prefixName = this.nzMenuService.isInDropDown ? 'ant-dropdown-menu-submenu' : 'ant-menu-submenu';
        this.nzUpdateHostClassService.updateHostClass(this.elementRef.nativeElement, {
            [`${prefixName}`]: true,
            [`${prefixName}-disabled`]: this.nzDisabled,
            [`${prefixName}-open`]: this.nzOpen,
            [`${prefixName}-selected`]: this.isChildMenuSelected,
            [`${prefixName}-${this.nzSubmenuService.mode}`]: true,
            [`${prefixName}-active`]: this.isMouseHover && !this.nzDisabled
        });
    }
    /**
     * @return {?}
     */
    ngOnInit() {
        combineLatest(this.nzSubmenuService.mode$, this.nzSubmenuService.open$)
            .pipe(takeUntil(this.destroy$))
            .subscribe((/**
         * @param {?} data
         * @return {?}
         */
        data => {
            /** @type {?} */
            const mode = data[0];
            /** @type {?} */
            const open = data[1];
            if (open && mode === 'inline') {
                this.expandState = 'expanded';
            }
            else if (open && mode === 'horizontal') {
                this.expandState = 'bottom';
            }
            else if (open && mode === 'vertical') {
                this.expandState = 'active';
            }
            else {
                this.expandState = 'collapsed';
            }
            this.overlayPositions =
                mode === 'horizontal' ? [POSITION_MAP.bottomLeft] : [POSITION_MAP.rightTop, POSITION_MAP.leftTop];
            if (open !== this.nzOpen) {
                this.nzOpen = open;
                this.nzOpenChange.emit(this.nzOpen);
            }
            this.setClassMap();
            this.setTriggerWidth();
        }));
        this.nzSubmenuService.menuOpen$.pipe(takeUntil(this.destroy$)).subscribe((/**
         * @param {?} data
         * @return {?}
         */
        (data) => {
            this.nzMenuService.menuOpen$.next(data);
        }));
        merge(this.nzMenuService.mode$, this.nzMenuService.inlineIndent$, this.nzSubmenuService.level$, this.nzSubmenuService.open$, this.nzSubmenuService.mode$)
            .pipe(takeUntil(this.destroy$))
            .subscribe((/**
         * @return {?}
         */
        () => {
            this.cdr.markForCheck();
        }));
    }
    /**
     * @return {?}
     */
    ngAfterContentInit() {
        this.setTriggerWidth();
        this.listOfNzMenuItemDirective.changes
            .pipe(startWith(true), flatMap((/**
         * @return {?}
         */
        () => merge(this.listOfNzMenuItemDirective.changes, ...this.listOfNzMenuItemDirective.map((/**
         * @param {?} menu
         * @return {?}
         */
        menu => menu.selected$))))), map((/**
         * @return {?}
         */
        () => this.listOfNzMenuItemDirective.some((/**
         * @param {?} e
         * @return {?}
         */
        e => e.nzSelected)))), takeUntil(this.destroy$))
            .subscribe((/**
         * @param {?} selected
         * @return {?}
         */
        selected => {
            this.isChildMenuSelected = selected;
            this.setClassMap();
        }));
    }
    /**
     * @param {?} changes
     * @return {?}
     */
    ngOnChanges(changes) {
        if (changes.nzOpen) {
            this.nzSubmenuService.setOpenState(this.nzOpen);
        }
        if (changes.nzDisabled) {
            this.nzSubmenuService.disabled = this.nzDisabled;
            this.setClassMap();
        }
    }
    /**
     * @return {?}
     */
    ngOnDestroy() {
        this.destroy$.next();
        this.destroy$.complete();
    }
}
NzSubMenuComponent.decorators = [
    { type: Component, args: [{
                selector: '[nz-submenu]',
                providers: [NzSubmenuService, NzUpdateHostClassService],
                animations: [collapseMotion, zoomBigMotion, slideMotion],
                encapsulation: ViewEncapsulation.None,
                changeDetection: ChangeDetectionStrategy.OnPush,
                preserveWhitespaces: false,
                template: "<div cdkOverlayOrigin\n  #origin=\"cdkOverlayOrigin\"\n  [class.ant-dropdown-menu-submenu-title]=\"nzMenuService.isInDropDown\"\n  [class.ant-menu-submenu-title]=\"!nzMenuService.isInDropDown\"\n  [style.paddingLeft.px]=\"nzMenuService.mode === 'inline'? (nzPaddingLeft ? nzPaddingLeft : nzSubmenuService.level * nzMenuService.inlineIndent) : null\"\n  (mouseenter)=\"setMouseEnterState(true)\"\n  (mouseleave)=\"setMouseEnterState(false)\"\n  (click)=\"clickSubMenuTitle()\">\n  <ng-content select=\"[title]\"></ng-content>\n  <span *ngIf=\"nzMenuService.isInDropDown; else notDropdownTpl\" class=\"ant-dropdown-menu-submenu-arrow\">\n    <i nz-icon type=\"right\" class=\"anticon-right ant-dropdown-menu-submenu-arrow-icon\"></i>\n  </span>\n  <ng-template #notDropdownTpl><i class=\"ant-menu-submenu-arrow\"></i></ng-template>\n</div>\n<ul *ngIf=\"nzMenuService.mode === 'inline'\"\n  [@collapseMotion]=\"expandState\"\n  [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n  [ngClass]=\"nzMenuClassName\"\n  class=\"ant-menu ant-menu-inline ant-menu-sub\">\n  <ng-template [ngTemplateOutlet]=\"subMenuTemplate\"></ng-template>\n</ul>\n<ng-template cdkConnectedOverlay\n  (positionChange)=\"onPositionChange($event)\"\n  [cdkConnectedOverlayPositions]=\"overlayPositions\"\n  [cdkConnectedOverlayOrigin]=\"origin\"\n  [cdkConnectedOverlayWidth]=\"triggerWidth\"\n  [cdkConnectedOverlayOpen]=\"nzOpen && nzMenuService.mode !== 'inline'\">\n  <div class=\"ant-menu-submenu ant-menu-submenu-popup\"\n    [@slideMotion]=\"expandState\"\n    [@zoomBigMotion]=\"expandState\"\n    [nzNoAnimation]=\"noAnimation?.nzNoAnimation\"\n    [class.ant-menu-light]=\"nzMenuService.theme === 'light'\"\n    [class.ant-menu-dark]=\"nzMenuService.theme === 'dark'\"\n    [class.ant-menu-submenu-placement-bottomLeft]=\"nzSubmenuService.mode === 'horizontal'\"\n    [class.ant-menu-submenu-placement-rightTop]=\"nzSubmenuService.mode === 'vertical' && placement === 'rightTop'\"\n    [class.ant-menu-submenu-placement-leftTop]=\"nzSubmenuService.mode === 'vertical' && placement === 'leftTop'\"\n    (mouseleave)=\"setMouseEnterState(false)\"\n    (mouseenter)=\"setMouseEnterState(true)\">\n    <ul [class.ant-dropdown-menu]=\"nzMenuService.isInDropDown\"\n      [class.ant-menu]=\"!nzMenuService.isInDropDown\"\n      [class.ant-dropdown-menu-vertical]=\"nzMenuService.isInDropDown\"\n      [class.ant-menu-vertical]=\"!nzMenuService.isInDropDown\"\n      [class.ant-dropdown-menu-sub]=\"nzMenuService.isInDropDown\"\n      [class.ant-menu-sub]=\"!nzMenuService.isInDropDown\"\n      [ngClass]=\"nzMenuClassName\">\n      <ng-template [ngTemplateOutlet]=\"subMenuTemplate\"></ng-template>\n    </ul>\n  </div>\n</ng-template>\n\n<ng-template #subMenuTemplate>\n  <ng-content></ng-content>\n</ng-template>",
                styles: [`
      .ant-menu-submenu-placement-bottomLeft {
        top: 6px;
        position: relative;
      }

      .ant-menu-submenu-placement-rightTop {
        left: 4px;
        position: relative;
      }

      .ant-menu-submenu-placement-leftTop {
        right: 4px;
        position: relative;
      }
    `]
            }] }
];
/** @nocollapse */
NzSubMenuComponent.ctorParameters = () => [
    { type: ElementRef },
    { type: NzMenuService },
    { type: ChangeDetectorRef },
    { type: NzSubmenuService },
    { type: NzUpdateHostClassService },
    { type: NzNoAnimationDirective, decorators: [{ type: Host }, { type: Optional }] }
];
NzSubMenuComponent.propDecorators = {
    nzMenuClassName: [{ type: Input }],
    nzPaddingLeft: [{ type: Input }],
    nzOpen: [{ type: Input }],
    nzDisabled: [{ type: Input }],
    nzOpenChange: [{ type: Output }],
    cdkConnectedOverlay: [{ type: ViewChild, args: [CdkConnectedOverlay,] }],
    cdkOverlayOrigin: [{ type: ViewChild, args: [CdkOverlayOrigin, { read: ElementRef },] }],
    listOfNzSubMenuComponent: [{ type: ContentChildren, args: [NzSubMenuComponent, { descendants: true },] }],
    listOfNzMenuItemDirective: [{ type: ContentChildren, args: [NzMenuItemDirective, { descendants: true },] }]
};
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSubMenuComponent.prototype, "nzOpen", void 0);
tslib_1.__decorate([
    InputBoolean(),
    tslib_1.__metadata("design:type", Object)
], NzSubMenuComponent.prototype, "nzDisabled", void 0);
if (false) {
    /** @type {?} */
    NzSubMenuComponent.prototype.nzMenuClassName;
    /** @type {?} */
    NzSubMenuComponent.prototype.nzPaddingLeft;
    /** @type {?} */
    NzSubMenuComponent.prototype.nzOpen;
    /** @type {?} */
    NzSubMenuComponent.prototype.nzDisabled;
    /** @type {?} */
    NzSubMenuComponent.prototype.nzOpenChange;
    /** @type {?} */
    NzSubMenuComponent.prototype.cdkConnectedOverlay;
    /** @type {?} */
    NzSubMenuComponent.prototype.cdkOverlayOrigin;
    /** @type {?} */
    NzSubMenuComponent.prototype.listOfNzSubMenuComponent;
    /** @type {?} */
    NzSubMenuComponent.prototype.listOfNzMenuItemDirective;
    /** @type {?} */
    NzSubMenuComponent.prototype.placement;
    /** @type {?} */
    NzSubMenuComponent.prototype.triggerWidth;
    /** @type {?} */
    NzSubMenuComponent.prototype.expandState;
    /** @type {?} */
    NzSubMenuComponent.prototype.overlayPositions;
    /**
     * @type {?}
     * @private
     */
    NzSubMenuComponent.prototype.destroy$;
    /**
     * @type {?}
     * @private
     */
    NzSubMenuComponent.prototype.isChildMenuSelected;
    /**
     * @type {?}
     * @private
     */
    NzSubMenuComponent.prototype.isMouseHover;
    /**
     * @type {?}
     * @private
     */
    NzSubMenuComponent.prototype.elementRef;
    /** @type {?} */
    NzSubMenuComponent.prototype.nzMenuService;
    /**
     * @type {?}
     * @private
     */
    NzSubMenuComponent.prototype.cdr;
    /** @type {?} */
    NzSubMenuComponent.prototype.nzSubmenuService;
    /**
     * @type {?}
     * @private
     */
    NzSubMenuComponent.prototype.nzUpdateHostClassService;
    /** @type {?} */
    NzSubMenuComponent.prototype.noAnimation;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotc3VibWVudS5jb21wb25lbnQuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsibWVudS9uei1zdWJtZW51LmNvbXBvbmVudC50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7OztBQUFBLE9BQU8sRUFBRSxtQkFBbUIsRUFBRSxnQkFBZ0IsRUFBa0MsTUFBTSxzQkFBc0IsQ0FBQztBQUM3RyxPQUFPLEVBRUwsdUJBQXVCLEVBQ3ZCLGlCQUFpQixFQUNqQixTQUFTLEVBQ1QsZUFBZSxFQUNmLFVBQVUsRUFDVixZQUFZLEVBQ1osSUFBSSxFQUNKLEtBQUssRUFJTCxRQUFRLEVBQ1IsTUFBTSxFQUNOLFNBQVMsRUFFVCxTQUFTLEVBQ1QsaUJBQWlCLEVBQ2xCLE1BQU0sZUFBZSxDQUFDO0FBRXZCLE9BQU8sRUFBRSxhQUFhLEVBQUUsS0FBSyxFQUFFLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUNyRCxPQUFPLEVBQUUsT0FBTyxFQUFFLEdBQUcsRUFBRSxTQUFTLEVBQUUsU0FBUyxFQUFFLE1BQU0sZ0JBQWdCLENBQUM7QUFDcEUsT0FBTyxFQUFFLGNBQWMsRUFBRSxNQUFNLDRCQUE0QixDQUFDO0FBQzVELE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSx5QkFBeUIsQ0FBQztBQUN0RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdkQsT0FBTyxFQUFFLHNCQUFzQixFQUFFLE1BQU0sZ0RBQWdELENBQUM7QUFDeEYsT0FBTyxFQUFFLGdCQUFnQixFQUFFLHlCQUF5QixFQUFFLFlBQVksRUFBRSxNQUFNLGtDQUFrQyxDQUFDO0FBQzdHLE9BQU8sRUFBRSx3QkFBd0IsRUFBRSxNQUFNLDRDQUE0QyxDQUFDO0FBQ3RGLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxzQkFBc0IsQ0FBQztBQUNwRCxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSwwQkFBMEIsQ0FBQztBQUMvRCxPQUFPLEVB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aW4sIENvbm5lY3RlZE92ZXJsYXlQb3NpdGlvbkNoYW5nZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7XG4gIEFmdGVyQ29udGVudEluaXQsXG4gIENoYW5nZURldGVjdGlvblN0cmF0ZWd5LFxuICBDaGFuZ2VEZXRlY3RvclJlZixcbiAgQ29tcG9uZW50LFxuICBDb250ZW50Q2hpbGRyZW4sXG4gIEVsZW1lbnRSZWYsXG4gIEV2ZW50RW1pdHRlcixcbiAgSG9zdCxcbiAgSW5wdXQsXG4gIE9uQ2hhbmdlcyxcbiAgT25EZXN0cm95LFxuICBPbkluaXQsXG4gIE9wdGlvbmFsLFxuICBPdXRwdXQsXG4gIFF1ZXJ5TGlzdCxcbiAgU2ltcGxlQ2hhbmdlcyxcbiAgVmlld0NoaWxkLFxuICBWaWV3RW5jYXBzdWxhdGlvblxufSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgY29tYmluZUxhdGVzdCwgbWVyZ2UsIFN1YmplY3QgfSBmcm9tICdyeGpzJztcbmltcG9ydCB7IGZsYXRNYXAsIG1hcCwgc3RhcnRXaXRoLCB0YWtlVW50aWwgfSBmcm9tICdyeGpzL29wZXJhdG9ycyc7XG5pbXBvcnQgeyBjb2xsYXBzZU1vdGlvbiB9IGZyb20gJy4uL2NvcmUvYW5pbWF0aW9uL2NvbGxhcHNlJztcbmltcG9ydCB7IHNsaWRlTW90aW9uIH0gZnJvbSAnLi4vY29yZS9hbmltYXRpb24vc2xpZGUnO1xuaW1wb3J0IHsgem9vbUJpZ01vdGlvbiB9IGZyb20gJy4uL2NvcmUvYW5pbWF0aW9uL3pvb20nO1xuaW1wb3J0IHsgTnpOb0FuaW1hdGlvbkRpcmVjdGl2ZSB9IGZyb20gJy4uL2NvcmUvbm8tYW5pbWF0aW9uL256LW5vLWFuaW1hdGlvbi5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgZ2V0UGxhY2VtZW50TmFtZSwgREVGQVVMVF9TVUJNRU5VX1BPU0lUSU9OUywgUE9TSVRJT05fTUFQIH0gZnJvbSAnLi4vY29yZS9vdmVybGF5L292ZXJsYXktcG9zaXRpb24nO1xuaW1wb3J0IHsgTnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlIH0gZnJvbSAnLi4vY29yZS9zZXJ2aWNlcy91cGRhdGUtaG9zdC1jbGFzcy5zZXJ2aWNlJztcbmltcG9ydCB7IElucHV0Qm9vbGVhbiB9IGZyb20gJy4uL2NvcmUvdXRpbC9jb252ZXJ0JztcbmltcG9ydCB7IE56TWVudUl0ZW1EaXJlY3RpdmUgfSBmcm9tICcuL256LW1lbnUtaXRlbS5kaXJlY3RpdmUnO1xuaW1wb3J0IHsgTnpNZW51U2VydmljZSB9IGZyb20gJy4vbnotbWVudS5zZXJ2aWNlJztcbmltcG9ydCB7IE56U3VibWVudVNlcnZpY2UgfSBmcm9tICcuL256LXN1Ym1lbnUuc2VydmljZSc7XG5cbkBDb21wb25lbnQoe1xuICBzZWxlY3RvcjogJ1tuei1zdWJtZW51XScsXG4gIHByb3ZpZGVyczogW056U3VibWVudVNlcnZpY2UsIE56VXBkYXRlSG9zdENsYXNzU2VydmljZV0sXG4gIGFuaW1hdGlvbnM6IFtjb2xsYXBzZU1vdGlvbiwgem9vbUJpZ01vdGlvbiwgc2xpZGVNb3Rpb25dLFxuICBlbmNhcHN1bGF0aW9uOiBWaWV3RW5jYXBzdWxhdGlvbi5Ob25lLFxuICBjaGFuZ2VEZXRlY3Rpb246IENoYW5nZURldGVjdGlvblN0cmF0ZWd5Lk9uUHVzaCxcbiAgcHJlc2VydmVXaGl0ZXNwYWNlczogZmFsc2UsXG4gIHRlbXBsYXRlVXJsOiAnLi9uei1zdWJtZW51LmNvbXBvbmVudC5odG1sJyxcbiAgc3R5bGVzOiBbXG4gICAgYFxuICAgICAgLmFudC1tZW51LXN1Ym1lbnUtcGxhY2VtZW50LWJvdHRvbUxlZnQge1xuICAgICAgICB0b3A6IDZweDtcbiAgICAgICAgcG9zaXRpb246IHJlbGF0aXZlO1xuICAgICAgfVxuXG4gICAgICAuYW50LW1lbnUtc3VibWVudS1wbGFjZW1lbnQtcmlnaHRUb3Age1xuICAgICAgICBsZWZ0OiA0cHg7XG4gICAgICAgIHBvc2l0aW9uOiByZWxhdGl2ZTtcbiAgICAgIH1cblxuICAgICAgLmFudC1tZW51LXN1Ym1lbnUtcGxhY2VtZW50LWxlZnRUb3Age1xuICAgICAgICByaWdodDogNHB4O1xuICAgICAgICBwb3NpdGlvbjogcmVsYXRpdmU7XG4gICAgICB9XG4gICAgYFxuICBdXG59KVxuZXhwb3J0IGNsYXNzIE56U3ViTWVudUNvbXBvbmVudCBpbXBsZW1lbnRzIE9uSW5pdCwgT25EZXN0cm95LCBBZnRlckNvbnRlbnRJbml0LCBPbkNoYW5nZXMge1xuICBASW5wdXQoKSBuek1lbnVDbGFzc05hbWU6IHN0cmluZztcbiAgQElucHV0KCkgbnpQYWRkaW5nTGVmdDogbnVtYmVyO1xuICBASW5wdXQoKSBASW5wdXRCb29sZWFuKCkgbnpPcGVuID0gZmFsc2U7XG4gIEBJbnB1dCgpIEBJbnB1dEJvb2xlYW4oKSBuekRpc2FibGVkID0gZmFsc2U7XG4gIEBPdXRwdXQoKSByZWFkb25seSBuek9wZW5DaGFuZ2U6IEV2ZW50RW1pdHRlcjxib29sZWFuPiA9IG5ldyBFdmVudEVtaXR0ZXIoKTtcblxuICBAVmlld0NoaWxkKENka0Nvbm5lY3RlZE92ZXJsYXkpIGNka0Nvbm5lY3RlZE92ZXJsYXk6IENka0Nvbm5lY3RlZE92ZXJsYXk7XG4gIEBWaWV3Q2hpbGQoQ2RrT3ZlcmxheU9yaWdpbiwgeyByZWFkOiBFbGVtZW50UmVmIH0pIGNka092ZXJsYXlPcmlnaW46IEVsZW1lbnRSZWY7XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpTdWJNZW51Q29tcG9uZW50LCB7IGRlc2NlbmRhbnRzOiB0cnVlIH0pXG4gIGxpc3RPZk56U3ViTWVudUNvbXBvbmVudDogUXVlcnlMaXN0PE56U3ViTWVudUNvbXBvbmVudD47XG4gIEBDb250ZW50Q2hpbGRyZW4oTnpNZW51SXRlbURpcmVjdGl2ZSwgeyBkZXNjZW5kYW50czogdHJ1ZSB9KVxuICBsaXN0T2ZOek1lbnVJdGVtRGlyZWN0aXZlOiBRdWVyeUxpc3Q8TnpNZW51SXRlbURpcmVjdGl2ZT47XG5cbiAgcGxhY2VtZW50ID0gJ3JpZ2h0VG9wJztcbiAgdHJpZ2dlcldpZHRoOiBudW1iZXI7XG4gIGV4cGFuZFN0YXRlID0gJ2NvbGxhcHNlZCc7XG4gIG92ZXJsYXlQb3NpdGlvbnMgPSBbLi4uREVGQVVMVF9TVUJNRU5VX1BPU0lUSU9OU107XG5cbiAgcHJpdmF0ZSBkZXN0cm95JCA9IG5ldyBTdWJqZWN0PHZvaWQ+KCk7XG4gIHByaXZhdGUgaXNDaGlsZE1lbnVTZWxlY3RlZCA9IGZhbHNlO1xuICBwcml2YXRlIGlzTW91c2VIb3ZlciA9IGZhbHNlO1xuXG4gIHNldE9wZW5TdGF0ZShvcGVuOiBib29sZWFuKTogdm9pZCB7XG4gICAgdGhpcy5uelN1Ym1lbnVTZXJ2aWNlLnNldE9wZW5TdGF0ZShvcGVuKTtcbiAgfVxuXG4gIGNsaWNrU3ViTWVudVRpdGxlKCk6IHZvaWQge1xuICAgIGlmICh0aGlzLm56U3VibWVudVNlcnZpY2UubW9kZSA9PT0gJ2lubGluZScgJiYgIXRoaXMubnpNZW51U2VydmljZS5pc0luRHJvcERvd24gJiYgIXRoaXMubnpEaXNhYmxlZCkge1xuICAgICAgdGhpcy5zZXRPcGVuU3RhdGUoIXRoaXMubnpPcGVuKTtcbiAgICB9XG4gIH1cblxuICBzZXRNb3VzZUVudGVyU3RhdGUodmFsdWU6IGJvb2xlYW4pOiB2b2lkIHtcbiAgICB0aGlzLmlzTW91c2VIb3ZlciA9IHZhbHVlO1xuICAgIHRoaXMuc2V0Q2xhc3NNYXAoKTtcbiAgICB0aGlzLm56U3VibWVudVNlcnZpY2Uuc2V0TW91c2VFbnRlclN0YXRlKHZhbHVlKTtcbiAgfVxuXG4gIHNldFRyaWdnZXJXaWR0aCgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5uelN1Ym1lbnVTZXJ2aWNlLm1vZGUgPT09ICdob3Jpem9udGFsJykge1xuICAgICAgdGhpcy50cmlnZ2VyV2lkdGggPSB0aGlzLmNka092ZXJsYXlPcmlnaW4ubmF0aXZlRWxlbWVudC5nZXRCb3VuZGluZ0NsaWVudFJlY3QoKS53aWR0aDtcbiAgICB9XG4gIH1cblxuICBvblBvc2l0aW9uQ2hhbmdlKHBvc2l0aW9uOiBDb25uZWN0ZWRPdmVybGF5UG9zaXRpb25DaGFuZ2UpOiB2b2lkIHtcbiAgICB0aGlzLnBsYWNlbWVudCA9IGdldFBsYWNlbWVudE5hbWUocG9zaXRpb24pITtcbiAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgfVxuXG4gIHNldENsYXNzTWFwKCk6IHZvaWQge1xuICAgIGNvbnN0IHByZWZpeE5hbWUgPSB0aGlzLm56TWVudVNlcnZpY2UuaXNJbkRyb3BEb3duID8gJ2FudC1kcm9wZG93bi1tZW51LXN1Ym1lbnUnIDogJ2FudC1tZW51LXN1Ym1lbnUnO1xuICAgIHRoaXMubnpVcGRhdGVIb3N0Q2xhc3NTZXJ2aWNlLnVwZGF0ZUhvc3RDbGFzcyh0aGlzLmVsZW1lbnRSZWYubmF0aXZlRWxlbWVudCwge1xuICAgICAgW2Ake3ByZWZpeE5hbWV9YF06IHRydWUsXG4gICAgICBbYCR7cHJlZml4TmFtZX0tZGlzYWJsZWRgXTogdGhpcy5uekRpc2FibGVkLFxuICAgICAgW2Ake3ByZWZpeE5hbWV9LW9wZW5gXTogdGhpcy5uek9wZW4sXG4gICAgICBbYCR7cHJlZml4TmFtZX0tc2VsZWN0ZWRgXTogdGhpcy5pc0NoaWxkTWVudVNlbGVjdGVkLFxuICAgICAgW2Ake3ByZWZpeE5hbWV9LSR7dGhpcy5uelN1Ym1lbnVTZXJ2aWNlLm1vZGV9YF06IHRydWUsXG4gICAgICBbYCR7cHJlZml4TmFtZX0tYWN0aXZlYF06IHRoaXMuaXNNb3VzZUhvdmVyICYmICF0aGlzLm56RGlzYWJsZWRcbiAgICB9KTtcbiAgfVxuXG4gIGNvbnN0cnVjdG9yKFxuICAgIHByaXZhdGUgZWxlbWVudFJlZjogRWxlbWVudFJlZixcbiAgICBwdWJsaWMgbnpNZW51U2VydmljZTogTnpNZW51U2VydmljZSxcbiAgICBwcml2YXRlIGNkcjogQ2hhbmdlRGV0ZWN0b3JSZWYsXG4gICAgcHVibGljIG56U3VibWVudVNlcnZpY2U6IE56U3VibWVudVNlcnZpY2UsXG4gICAgcHJpdmF0ZSBuelVwZGF0ZUhvc3RDbGFzc1NlcnZpY2U6IE56VXBkYXRlSG9zdENsYXNzU2VydmljZSxcbiAgICBASG9zdCgpIEBPcHRpb25hbCgpIHB1YmxpYyBub0FuaW1hdGlvbj86IE56Tm9BbmltYXRpb25EaXJlY3RpdmVcbiAgKSB7fVxuXG4gIG5nT25Jbml0KCk6IHZvaWQge1xuICAgIGNvbWJpbmVMYXRlc3QodGhpcy5uelN1Ym1lbnVTZXJ2aWNlLm1vZGUkLCB0aGlzLm56U3VibWVudVNlcnZpY2Uub3BlbiQpXG4gICAgICAucGlwZSh0YWtlVW50aWwodGhpcy5kZXN0cm95JCkpXG4gICAgICAuc3Vic2NyaWJlKGRhdGEgPT4ge1xuICAgICAgICBjb25zdCBtb2RlID0gZGF0YVswXTtcbiAgICAgICAgY29uc3Qgb3BlbiA9IGRhdGFbMV07XG4gICAgICAgIGlmIChvcGVuICYmIG1vZGUgPT09ICdpbmxpbmUnKSB7XG4gICAgICAgICAgdGhpcy5leHBhbmRTdGF0ZSA9ICdleHBhbmRlZCc7XG4gICAgICAgIH0gZWxzZSBpZiAob3BlbiAmJiBtb2RlID09PSAnaG9yaXpvbnRhbCcpIHtcbiAgICAgICAgICB0aGlzLmV4cGFuZFN0YXRlID0gJ2JvdHRvbSc7XG4gICAgICAgIH0gZWxzZSBpZiAob3BlbiAmJiBtb2RlID09PSAndmVydGljYWwnKSB7XG4gICAgICAgICAgdGhpcy5leHBhbmRTdGF0ZSA9ICdhY3RpdmUnO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRoaXMuZXhwYW5kU3RhdGUgPSAnY29sbGFwc2VkJztcbiAgICAgICAgfVxuICAgICAgICB0aGlzLm92ZXJsYXlQb3NpdGlvbnMgPVxuICAgICAgICAgIG1vZGUgPT09ICdob3Jpem9udGFsJyA/IFtQT1NJVElPTl9NQVAuYm90dG9tTGVmdF0gOiBbUE9TSVRJT05fTUFQLnJpZ2h0VG9wLCBQT1NJVElPTl9NQVAubGVmdFRvcF07XG4gICAgICAgIGlmIChvcGVuICE9PSB0aGlzLm56T3Blbikge1xuICAgICAgICAgIHRoaXMubnpPcGVuID0gb3BlbjtcbiAgICAgICAgICB0aGlzLm56T3BlbkNoYW5nZS5lbWl0KHRoaXMubnpPcGVuKTtcbiAgICAgICAgfVxuICAgICAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gICAgICAgIHRoaXMuc2V0VHJpZ2dlcldpZHRoKCk7XG4gICAgICB9KTtcbiAgICB0aGlzLm56U3VibWVudVNlcnZpY2UubWVudU9wZW4kLnBpcGUodGFrZVVudGlsKHRoaXMuZGVzdHJveSQpKS5zdWJzY3JpYmUoKGRhdGE6IGJvb2xlYW4pID0+IHtcbiAgICAgIHRoaXMubnpNZW51U2VydmljZS5tZW51T3BlbiQubmV4dChkYXRhKTtcbiAgICB9KTtcbiAgICBtZXJnZShcbiAgICAgIHRoaXMubnpNZW51U2VydmljZS5tb2RlJCxcbiAgICAgIHRoaXMubnpNZW51U2VydmljZS5pbmxpbmVJbmRlbnQkLFxuICAgICAgdGhpcy5uelN1Ym1lbnVTZXJ2aWNlLmxldmVsJCxcbiAgICAgIHRoaXMubnpTdWJtZW51U2VydmljZS5vcGVuJCxcbiAgICAgIHRoaXMubnpTdWJtZW51U2VydmljZS5tb2RlJFxuICAgIClcbiAgICAgIC5waXBlKHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKSlcbiAgICAgIC5zdWJzY3JpYmUoKCkgPT4ge1xuICAgICAgICB0aGlzLmNkci5tYXJrRm9yQ2hlY2soKTtcbiAgICAgIH0pO1xuICB9XG5cbiAgbmdBZnRlckNvbnRlbnRJbml0KCk6IHZvaWQge1xuICAgIHRoaXMuc2V0VHJpZ2dlcldpZHRoKCk7XG4gICAgdGhpcy5saXN0T2ZOek1lbnVJdGVtRGlyZWN0aXZlLmNoYW5nZXNcbiAgICAgIC5waXBlKFxuICAgICAgICBzdGFydFdpdGgodHJ1ZSksXG4gICAgICAgIGZsYXRNYXAoKCkgPT5cbiAgICAgICAgICBtZXJnZSh0aGlzLmxpc3RPZk56TWVudUl0ZW1EaXJlY3RpdmUuY2hhbmdlcywgLi4udGhpcy5saXN0T2ZOek1lbnVJdGVtRGlyZWN0aXZlLm1hcChtZW51ID0+IG1lbnUuc2VsZWN0ZWQkKSlcbiAgICAgICAgKSxcbiAgICAgICAgbWFwKCgpID0+IHRoaXMubGlzdE9mTnpNZW51SXRlbURpcmVjdGl2ZS5zb21lKGUgPT4gZS5uelNlbGVjdGVkKSksXG4gICAgICAgIHRha2VVbnRpbCh0aGlzLmRlc3Ryb3kkKVxuICAgICAgKVxuICAgICAgLnN1YnNjcmliZShzZWxlY3RlZCA9PiB7XG4gICAgICAgIHRoaXMuaXNDaGlsZE1lbnVTZWxlY3RlZCA9IHNlbGVjdGVkO1xuICAgICAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gICAgICB9KTtcbiAgfVxuXG4gIG5nT25DaGFuZ2VzKGNoYW5nZXM6IFNpbXBsZUNoYW5nZXMpOiB2b2lkIHtcbiAgICBpZiAoY2hhbmdlcy5uek9wZW4pIHtcbiAgICAgIHRoaXMubnpTdWJtZW51U2VydmljZS5zZXRPcGVuU3RhdGUodGhpcy5uek9wZW4pO1xuICAgIH1cbiAgICBpZiAoY2hhbmdlcy5uekRpc2FibGVkKSB7XG4gICAgICB0aGlzLm56U3VibWVudVNlcnZpY2UuZGlzYWJsZWQgPSB0aGlzLm56RGlzYWJsZWQ7XG4gICAgICB0aGlzLnNldENsYXNzTWFwKCk7XG4gICAgfVxuICB9XG5cbiAgbmdPbkRlc3Ryb3koKTogdm9pZCB7XG4gICAgdGhpcy5kZXN0cm95JC5uZXh0KCk7XG4gICAgdGhpcy5kZXN0cm95JC5jb21wbGV0ZSgpO1xuICB9XG59XG4iXX0=