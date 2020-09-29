/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzPopoverComponent } from './nz-popover.component';
import { NzPopoverDirective } from './nz-popover.directive';
var NzPopoverModule = /** @class */ (function () {
    function NzPopoverModule() {
    }
    NzPopoverModule.decorators = [
        { type: NgModule, args: [{
                    entryComponents: [NzPopoverComponent],
                    exports: [NzPopoverDirective, NzPopoverComponent],
                    declarations: [NzPopoverDirective, NzPopoverComponent],
                    imports: [CommonModule, OverlayModule, NzAddOnModule, NzOverlayModule, NzNoAnimationModule]
                },] }
    ];
    return NzPopoverModule;
}());
export { NzPopoverModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wb3Zlci5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wb3Zlci9uei1wb3BvdmVyLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXpDLE9BQU8sRUFBRSxhQUFhLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUMzRCxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSw2Q0FBNkMsQ0FBQztBQUNsRixPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFFcEUsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDNUQsT0FBTyxFQUFFLGtCQUFrQixFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFFNUQ7SUFBQTtJQU04QixDQUFDOztnQkFOOUIsUUFBUSxTQUFDO29CQUNSLGVBQWUsRUFBRSxDQUFDLGtCQUFrQixDQUFDO29CQUNyQyxPQUFPLEVBQUUsQ0FBQyxrQkFBa0IsRUFBRSxrQkFBa0IsQ0FBQztvQkFDakQsWUFBWSxFQUFFLENBQUMsa0JBQWtCLEVBQUUsa0JBQWtCLENBQUM7b0JBQ3RELE9BQU8sRUFBRSxDQUFDLFlBQVksRUFBRSxhQUFhLEVBQUUsYUFBYSxFQUFFLGVBQWUsRUFBRSxtQkFBbUIsQ0FBQztpQkFDNUY7O0lBQzZCLHNCQUFDO0NBQUEsQUFOL0IsSUFNK0I7U0FBbEIsZUFBZSIsInNvdXJjZXNDb250ZW50IjpbImltcG9ydCB7IE92ZXJsYXlNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jZGsvb3ZlcmxheSc7XG5pbXBvcnQgeyBDb21tb25Nb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb21tb24nO1xuaW1wb3J0IHsgTmdNb2R1bGUgfSBmcm9tICdAYW5ndWxhci9jb3JlJztcblxuaW1wb3J0IHsgTnpBZGRPbk1vZHVsZSB9IGZyb20gJy4uL2NvcmUvYWRkb24vYWRkb24ubW9kdWxlJztcbmltcG9ydCB7IE56Tm9BbmltYXRpb25Nb2R1bGUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24ubW9kdWxlJztcbmltcG9ydCB7IE56T3ZlcmxheU1vZHVsZSB9IGZyb20gJy4uL2NvcmUvb3ZlcmxheS9uei1vdmVybGF5Lm1vZHVsZSc7XG5cbmltcG9ydCB7IE56UG9wb3ZlckNvbXBvbmVudCB9IGZyb20gJy4vbnotcG9wb3Zlci5jb21wb25lbnQnO1xuaW1wb3J0IHsgTnpQb3BvdmVyRGlyZWN0aXZlIH0gZnJvbSAnLi9uei1wb3BvdmVyLmRpcmVjdGl2ZSc7XG5cbkBOZ01vZHVsZSh7XG4gIGVudHJ5Q29tcG9uZW50czogW056UG9wb3ZlckNvbXBvbmVudF0sXG4gIGV4cG9ydHM6IFtOelBvcG92ZXJEaXJlY3RpdmUsIE56UG9wb3ZlckNvbXBvbmVudF0sXG4gIGRlY2xhcmF0aW9uczogW056UG9wb3ZlckRpcmVjdGl2ZSwgTnpQb3BvdmVyQ29tcG9uZW50XSxcbiAgaW1wb3J0czogW0NvbW1vbk1vZHVsZSwgT3ZlcmxheU1vZHVsZSwgTnpBZGRPbk1vZHVsZSwgTnpPdmVybGF5TW9kdWxlLCBOek5vQW5pbWF0aW9uTW9kdWxlXVxufSlcbmV4cG9ydCBjbGFzcyBOelBvcG92ZXJNb2R1bGUge31cbiJdfQ==